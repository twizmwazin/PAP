use std::{borrow::BorrowMut, marker::PhantomData};

use icicle_vm::{Snapshot, Vm};
use libafl_bolts::tuples::RefIndexable;

use libafl::{
    corpus::Corpus,
    events::{EventFirer, EventRestarter},
    executors::{Executor, ExitKind, HasObservers},
    feedbacks::Feedback,
    fuzzer::HasObjective,
    observers::ObserversTuple,
    state::{HasCorpus, HasExecutions, HasSolutions, State, UsesState},
    Error,
};

pub struct IcicleInProcessExecutor<H, OT, S>
where
    H: FnMut(&mut Vm, &S::Input) -> ExitKind,
    OT: ObserversTuple<S::Input, S>,
    S: State,
{
    vm: Vm,
    harness_fn: H,
    observers: OT,
    snapshot: Snapshot,
    phantom: PhantomData<(*const S,)>,
}

impl<H, OT, S> UsesState for IcicleInProcessExecutor<H, OT, S>
where
    H: FnMut(&mut Vm, &S::Input) -> ExitKind,
    OT: ObserversTuple<S::Input, S>,
    S: State,
{
    type State = S;
}

impl<EM, H, OT, S, Z> Executor<EM, Z> for IcicleInProcessExecutor<H, OT, S>
where
    EM: UsesState<State = S>,
    H: FnMut(&mut Vm, &S::Input) -> ExitKind,
    OT: ObserversTuple<S::Input, S>,
    S: State + HasExecutions,
    Z: UsesState<State = S>,
{
    fn run_target(
        &mut self,
        _fuzzer: &mut Z,
        state: &mut Self::State,
        _mgr: &mut EM,
        input: &Self::Input,
    ) -> Result<ExitKind, Error> {
        *state.executions_mut() += 1;

        let ret = self.harness_fn.borrow_mut()(&mut self.vm, input);

        self.vm.restore(&self.snapshot);

        Ok(ret)
    }
}

impl<H, OT, S> HasObservers for IcicleInProcessExecutor<H, OT, S>
where
    H: FnMut(&mut icicle_vm::Vm, &S::Input) -> ExitKind,
    OT: ObserversTuple<S::Input, S>,
    S: State,
{
    type Observers = OT;

    #[inline]
    fn observers(&self) -> RefIndexable<&Self::Observers, Self::Observers> {
        RefIndexable::from(&self.observers)
    }

    #[inline]
    fn observers_mut(&mut self) -> RefIndexable<&mut Self::Observers, Self::Observers> {
        RefIndexable::from(&mut self.observers)
    }
}

impl<H, OT, S> IcicleInProcessExecutor<H, OT, S>
where
    H: FnMut(&mut Vm, &S::Input) -> ExitKind,
    OT: ObserversTuple<S::Input, S>,
    S: State + HasExecutions + HasSolutions + HasCorpus,
    <S as HasSolutions>::Solutions: Corpus<Input = S::Input>, //delete me
    <<S as HasCorpus>::Corpus as Corpus>::Input: Clone,       //delete me
{
    pub fn new<EM, OF, Z>(
        mut vm: Vm,
        harness_fn: H,
        observers: OT,
        _fuzzer: &mut Z,
        _state: &mut S,
        _event_mgr: &mut EM,
    ) -> Result<Self, Error>
    where
        Self: Executor<EM, Z, State = S>,
        EM: EventFirer<State = S> + EventRestarter,
        OF: Feedback<EM, S::Input, OT, S>,
        S: State,
        Z: HasObjective<Objective = OF, State = S>,
        <S as HasSolutions>::Solutions: Corpus<Input = S::Input>, //delete me
        <<S as HasCorpus>::Corpus as Corpus>::Input: Clone,       //delete me
    {
        let snapshot = vm.snapshot();
        Ok(Self {
            vm,
            harness_fn,
            observers,
            snapshot,
            phantom: PhantomData,
        })
    }
}
