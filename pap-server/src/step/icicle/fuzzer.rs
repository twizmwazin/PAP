use std::cmp::max;
use std::num::NonZero;
use std::rc::Rc;
use std::sync::RwLock;

use anyhow::anyhow;
use anyhow::Result;
use icicle_fuzzing::coverage::register_afl_hit_counts_all;
use icicle_vm::cpu::mem::perm::{EXEC, READ, WRITE};
use icicle_vm::cpu::mem::Mapping;
use icicle_vm::cpu::{Config, ExceptionCode};
use icicle_vm::Vm;
use icicle_vm::VmExit;
use libafl::feedbacks::MaxMapFeedback;
use libafl::generators::RandBytesGenerator;
use libafl::inputs::HasMutatorBytes;
use libafl::monitors::SimpleMonitor;
use libafl::observers::{CanTrack, ConstMapObserver, HitcountsMapObserver};
use libafl::stages::StdMutationalStage;
use libafl::{
    events::SimpleEventManager,
    executors::ExitKind,
    feedbacks::CrashFeedback,
    fuzzer::{Fuzzer, StdFuzzer},
    inputs::BytesInput,
    mutators::{havoc_mutations::havoc_mutations, scheduled::StdScheduledMutator},
    schedulers::QueueScheduler,
    state::StdState,
};
use libafl_bolts::HasLen;
use libafl_bolts::{current_nanos, rands::StdRand, tuples::tuple_list};
use libafl_targets::EDGES_MAP_DEFAULT_SIZE;
use mlua::Error;
use mlua::UserData;

use crate::step::icicle::sqlcorpus::SqlCorpus;
use crate::step::StepContext;

#[inline]
fn vm_reg(vm: &Vm, reg: &str) -> pcode::VarNode {
    vm.cpu.arch.sleigh.get_reg(reg).unwrap().var
}

static mut EDGES_MAP: [u8; EDGES_MAP_DEFAULT_SIZE] = [0; EDGES_MAP_DEFAULT_SIZE];

struct LuaVmBridge<'a> {
    vm: RwLock<&'a mut Vm>,
}

impl UserData for LuaVmBridge<'_> {
    fn add_methods<M: mlua::UserDataMethods<Self>>(methods: &mut M) {
        methods.add_method("read_mem", |_, this, (offset, len): (u64, u64)| {
            let mut buf = Vec::with_capacity(len as usize);
            Ok(this
                .vm
                .write()
                .expect("lock poisoned")
                .cpu
                .mem
                .read_bytes(offset, &mut buf, READ)
                .map_err(|e| anyhow!("{}", e))?)
        });

        methods.add_method("write_mem", |_, this, (offset, data): (u64, Vec<u8>)| {
            this.vm
                .write()
                .expect("lock poisoned")
                .cpu
                .mem
                .write_bytes(offset, &data, WRITE)
                .map_err(Error::external)
        });

        methods.add_method("set_reg", |_, this, (reg_name, value): (String, u64)| {
            let mut vm = this.vm.write().expect("lock poisoned");
            let reg = vm_reg(*vm, &reg_name);
            vm.cpu.write_reg(reg, value);
            Ok(())
        });
    }
}

#[derive(Clone)]
struct RhaiVmBridge<'a>(Rc<RwLock<&'a mut Vm>>);

impl<'a> RhaiVmBridge<'a> {
    fn read_mem_u32(&mut self, offset: i64) -> i64 {
        let mut vm = self.0.write().expect("lock poisoned");
        let mut buf = [0u8; 4];
        vm.cpu
            .mem
            .read_bytes(offset as u64, &mut buf, READ)
            .unwrap_or_else(|_| panic!("failed to read memory at 0x{:x}", offset));

        if vm.cpu.arch.sleigh.big_endian {
            u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as i64
        } else {
            u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as i64
        }
    }

    fn write_reg(&mut self, reg_name: String, value: i64) {
        let mut vm = self.0.write().expect("lock poisoned");
        let reg = vm_reg(&vm, &reg_name);
        vm.cpu.write_reg(reg, value as u64);
    }
}

fn run_rhai_harness(vm: &mut Vm, harness_code: &str) -> Result<()> {
    let static_vm = unsafe { std::mem::transmute::<&mut Vm, &'static mut Vm>(vm) };
    let mut engine = rhai::Engine::new();
    let vm = RhaiVmBridge(Rc::new(RwLock::new(static_vm)));

    engine
        .register_type::<RhaiVmBridge>()
        .register_fn("read_mem_u32", RhaiVmBridge::read_mem_u32)
        .register_fn("write_reg", RhaiVmBridge::write_reg);

    let mut scope = rhai::Scope::new();
    scope.push_constant("input_addr", 0x4100_0000_i64);
    scope.push("vm", vm);

    engine
        .eval_with_scope::<()>(&mut scope, harness_code)
        .expect("failed to run harness");

    Ok(())
}

struct FuzzHarness {
    input_addr: u64,
    func_addr: u64,
    return_addr: u64,
    stack_addr: u64,
    lua_code: String,
}

impl FuzzHarness {
    fn new(input_addr: u64, func_addr: u64, stack_addr: u64, lua_code: String) -> Self {
        Self {
            input_addr,
            func_addr,
            return_addr: 0x1336,
            stack_addr,
            lua_code,
        }
    }

    fn setup_input(&self, vm: &mut Vm, input: &[u8]) -> Result<()> {
        // Map input memory region
        let length = max(input.len() as u64 + 1, 0x1000);
        vm.cpu.mem.map_memory_len(
            self.input_addr,
            length,
            Mapping {
                perm: READ,
                value: 0,
            },
        );
        vm.cpu.mem.write_bytes(self.input_addr, input, READ)?;
        vm.cpu
            .mem
            .write_u8(self.input_addr + input.len() as u64, 0, READ)?;
        Ok(())
    }

    fn setup_registers(&self, vm: &mut Vm) -> Result<()> {
        // Set up base CPU state
        // println!("writing pc: 0x{:x}", self.func_addr);
        vm.cpu.write_pc(self.func_addr);
        vm.cpu.write_reg(vm_reg(vm, "sp"), self.stack_addr);
        vm.cpu.write_reg(vm_reg(vm, "lr"), self.return_addr);

        // Run harness
        run_rhai_harness(vm, &self.lua_code)?;

        Ok(())
    }
}

pub fn fuzz(ctx: &StepContext) -> Result<()> {
    // Get project configuration
    let project = get_project(ctx)?;
    let loader = project
        .loader
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("no loader configuration"))?;

    // Parse function address
    let function = ctx
        .get_arg("function")
        .ok_or(anyhow!("Missing function arg"))?;
    let fuzz_func_addr = u64::from_str_radix(function.trim_start_matches("0x"), 16)?;

    // Setup harness
    let harness_config = ctx
        .get_arg("harness")
        .ok_or(anyhow!("Missing harness arg"))?;
    let input_addr = ctx
        .get_arg("input_addr")
        .map(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16))
        .unwrap_or(Ok(0x4100_0000))?;
    let harness = FuzzHarness::new(
        input_addr,
        fuzz_func_addr,
        loader.stack_address,
        harness_config.to_string(),
    );

    // Configure and setup VM
    let mut vm = {
        let config = Config {
            enable_jit: false,
            enable_jit_mem: false,
            enable_recompilation: false,
            enable_shadow_stack: false,
            ..icicle_vm::cpu::Config::from_target_triple(project.arch.as_str())
        };
        let mut vm = icicle_vm::build(&config)?;

        // Load binary
        let binary = ctx
            .get_file(&project.binary)
            .ok_or_else(|| anyhow!("missing binary file"))?;
        let rwx = READ | WRITE | EXEC;
        vm.cpu.mem.map_memory_len(
            loader.base_address,
            binary.len() as u64,
            Mapping {
                perm: rwx,
                value: 0,
            },
        );
        vm.cpu.mem.write_bytes(loader.base_address, binary, rwx)?;

        // Setup memory regions
        vm.cpu.mem.map_memory_len(
            loader.stack_address - 0x500_0000,
            0x500_0000,
            Mapping {
                perm: READ | WRITE,
                value: 0,
            },
        );

        // Initialize MMIO regions from project config
        for region in &project.mmio {
            vm.cpu.mem.map_memory_len(
                region.address,
                0x1000, // TODO: Make size configurable
                Mapping {
                    perm: READ | WRITE,
                    value: 1,
                },
            );
            // TODO: Handle different MMIO handlers
            vm.cpu.mem.write_u32(region.address, 0, READ | WRITE)?;
        }

        vm
    };

    // Create harness closure with minimal error handling
    let mut harness_fn = |vm: &mut Vm, input: &BytesInput| -> ExitKind {
        if input.len() < 8 {
            return ExitKind::Ok;
        }

        // Ignore potential errors in harness - just treat them as crashes
        if harness.setup_input(vm, input.bytes()).is_err() {
            log::error!("Failed to setup input");
            return ExitKind::Crash;
        }
        if let Err(e) = harness.setup_registers(vm) {
            log::error!("Harness is broken: {}", e);
            return ExitKind::Crash;
        }

        let vm_result = vm.run_until(harness.return_addr);

        match vm_result {
            VmExit::Running => ExitKind::Ok,
            VmExit::InstructionLimit => ExitKind::Timeout,
            VmExit::Breakpoint => ExitKind::Ok,
            VmExit::Interrupted => ExitKind::Timeout,
            VmExit::Halt => ExitKind::Crash,
            VmExit::Killed => ExitKind::Crash,
            VmExit::Deadlock => ExitKind::Crash,
            VmExit::OutOfMemory => ExitKind::Oom,
            VmExit::Unimplemented => ExitKind::Timeout,
            VmExit::UnhandledException(e) => {
                if matches!(e, (ExceptionCode::ExecViolation, 0x1336)) {
                    ExitKind::Ok
                } else {
                    ExitKind::Crash
                }
            }
        }
    };

    // Get output paths from IO configuration
    let output_io = ctx
        .get_io("output")
        .ok_or_else(|| anyhow::anyhow!("missing output directory"))?
        .to_string();
    let solutions_io = ctx
        .get_io("solutions")
        .ok_or_else(|| anyhow::anyhow!("missing solutions directory"))?
        .to_string();

    // Setup LibAFL components
    #[allow(static_mut_refs)]
    let edges_observer = unsafe {
        HitcountsMapObserver::new(ConstMapObserver::<_, EDGES_MAP_DEFAULT_SIZE>::new(
            "edges",
            &mut EDGES_MAP,
        ))
        .track_indices()
    };
    register_afl_hit_counts_all(
        &mut vm,
        unsafe { EDGES_MAP.as_mut_ptr() },
        EDGES_MAP_DEFAULT_SIZE as u32,
    );

    let mut feedback = MaxMapFeedback::new(&edges_observer);
    let mut objective = CrashFeedback::new();

    // Create corpus instances with appropriate namespaces
    let main_corpus = SqlCorpus::new(output_io);
    let solutions_corpus = SqlCorpus::new(solutions_io);

    let mut state = StdState::new(
        StdRand::with_seed(current_nanos()),
        main_corpus,
        solutions_corpus,
        &mut feedback,
        &mut objective,
    )?;

    let mon = SimpleMonitor::new(|s| ctx.log(s));
    let mut mgr = SimpleEventManager::new(mon);
    let scheduler = QueueScheduler::new();
    let mut fuzzer = StdFuzzer::new(scheduler, feedback, objective);

    let mut executor = super::executor::IcicleInProcessExecutor::new(
        vm,
        &mut harness_fn,
        tuple_list!(edges_observer),
        &mut fuzzer,
        &mut state,
        &mut mgr,
    )?;

    // Generate initial corpus
    let mut generator = RandBytesGenerator::new(unsafe { NonZero::new_unchecked(128) });
    state
        .generate_initial_inputs(&mut fuzzer, &mut executor, &mut generator, &mut mgr, 64)
        .expect("rut roh");

    let mutator = StdScheduledMutator::new(havoc_mutations());
    let mut stages = tuple_list!(StdMutationalStage::new(mutator));

    loop {
        if ctx.is_cancelled() {
            break;
        }
        fuzzer.fuzz_loop_for(&mut stages, &mut executor, &mut state, &mut mgr, 10)?;
    }

    Ok(())
}

fn get_project<'a>(ctx: &'a StepContext) -> Result<&'a pap_api::Project> {
    let project_name = ctx
        .get_arg("project")
        .ok_or_else(|| anyhow::anyhow!("missing `project` argument"))?;

    ctx.pipeline_status
        .config
        .projects
        .iter()
        .find(|p| p.name == project_name)
        .ok_or_else(|| anyhow::anyhow!("project '{}' not found", project_name))
}
