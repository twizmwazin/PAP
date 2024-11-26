use libafl::{
    corpus::{Corpus, CorpusId, Testcase},
    inputs::{BytesInput, HasMutatorBytes},
    Error,
};
use serde::{Deserialize, Serialize};
use std::{cell::RefCell, collections::HashSet};
use tokio::runtime::Handle;

#[derive(Serialize, Deserialize)]
pub struct SqlCorpus {
    namespace: String,
    current: Option<CorpusId>,
    cached_ids: HashSet<CorpusId>,
    disabled_ids: HashSet<CorpusId>,
    testcases: Vec<RefCell<Testcase<BytesInput>>>,
}

impl SqlCorpus {
    pub fn new(namespace: String) -> Self {
        Self {
            namespace,
            current: None,
            cached_ids: HashSet::new(),
            disabled_ids: HashSet::new(),
            testcases: Vec::new(),
        }
    }

    fn make_key(&self, id: usize) -> Vec<u8> {
        id.to_be_bytes().to_vec()
    }

    fn write_object(&self, key: &[u8], data: &[u8]) -> Result<(), Error> {
        Handle::current()
            .block_on(async { crate::queries::put_object(&self.namespace, key, data).await })
            .map_err(|e| Error::illegal_state(format!("Failed to store testcase: {}", e)))
    }

    fn read_object(&self, key: &[u8]) -> Result<Vec<u8>, Error> {
        Handle::current()
            .block_on(async { crate::queries::get_object(&self.namespace, key).await })
            .map_err(|e| Error::illegal_state(format!("Failed to load testcase: {}", e)))
    }
}

impl Corpus for SqlCorpus {
    type Input = BytesInput;

    fn count(&self) -> usize {
        self.cached_ids.len() - self.disabled_ids.len()
    }

    fn count_disabled(&self) -> usize {
        self.disabled_ids.len()
    }

    fn count_all(&self) -> usize {
        self.cached_ids.len()
    }

    fn add(&mut self, testcase: Testcase<BytesInput>) -> Result<CorpusId, Error> {
        let id = CorpusId::from(self.testcases.len());

        let input_bytes = testcase
            .input()
            .as_ref()
            .ok_or_else(|| Error::illegal_state("Cannot add testcase with None input"))?
            .bytes();

        // Store testcase data using context with our namespace
        self.write_object(&self.make_key(id.0), input_bytes)?;

        self.testcases.push(RefCell::new(testcase));
        self.cached_ids.insert(id);
        Ok(id)
    }

    fn add_disabled(&mut self, testcase: Testcase<BytesInput>) -> Result<CorpusId, Error> {
        let id = self.add(testcase)?;
        self.disabled_ids.insert(id);
        Ok(id)
    }

    fn replace(
        &mut self,
        id: CorpusId,
        testcase: Testcase<BytesInput>,
    ) -> Result<Testcase<BytesInput>, Error> {
        if !self.cached_ids.contains(&id) {
            return Err(Error::key_not_found("Corpus entry not found"));
        }

        let input_bytes = testcase
            .input()
            .as_ref()
            .ok_or_else(|| Error::illegal_state("Cannot replace testcase with None input"))?
            .bytes();

        // Store using context with our namespace
        self.write_object(&self.make_key(id.0), input_bytes)?;

        let old = std::mem::replace(&mut *self.testcases[id.0].borrow_mut(), testcase);
        Ok(old)
    }

    fn remove(&mut self, id: CorpusId) -> Result<Testcase<BytesInput>, Error> {
        if !self.cached_ids.contains(&id) {
            return Err(Error::key_not_found("Corpus entry not found"));
        }

        // Remove using context with our namespace
        self.write_object(&self.make_key(id.0), &[])?;

        self.cached_ids.remove(&id);
        if self.disabled_ids.contains(&id) {
            self.disabled_ids.remove(&id);
        }

        Ok(Testcase::new(BytesInput::new(Vec::new())))
    }

    fn get(&self, id: CorpusId) -> Result<&RefCell<Testcase<BytesInput>>, Error> {
        if !self.cached_ids.contains(&id) || self.disabled_ids.contains(&id) {
            return Err(Error::key_not_found("Corpus entry not found or disabled"));
        }
        Ok(&self.testcases[id.0])
    }

    fn get_from_all(&self, id: CorpusId) -> Result<&RefCell<Testcase<BytesInput>>, Error> {
        if !self.cached_ids.contains(&id) {
            return Err(Error::key_not_found("Corpus entry not found"));
        }
        Ok(&self.testcases[id.0])
    }

    fn current(&self) -> &Option<CorpusId> {
        &self.current
    }

    fn current_mut(&mut self) -> &mut Option<CorpusId> {
        &mut self.current
    }

    fn next(&self, id: CorpusId) -> Option<CorpusId> {
        let next_id = CorpusId::from(id.0 + 1);
        if self.cached_ids.contains(&next_id) && !self.disabled_ids.contains(&next_id) {
            Some(next_id)
        } else {
            None
        }
    }

    fn prev(&self, id: CorpusId) -> Option<CorpusId> {
        if id.0 == 0 {
            return None;
        }
        let prev_id = CorpusId::from(id.0 - 1);
        if self.cached_ids.contains(&prev_id) && !self.disabled_ids.contains(&prev_id) {
            Some(prev_id)
        } else {
            None
        }
    }

    fn first(&self) -> Option<CorpusId> {
        self.cached_ids
            .iter()
            .find(|id| !self.disabled_ids.contains(id))
            .copied()
    }

    fn last(&self) -> Option<CorpusId> {
        self.cached_ids
            .iter()
            .filter(|id| !self.disabled_ids.contains(id))
            .max()
            .copied()
    }

    fn nth_from_all(&self, nth: usize) -> CorpusId {
        CorpusId::from(nth)
    }

    fn peek_free_id(&self) -> CorpusId {
        CorpusId::from(self.testcases.len())
    }

    fn load_input_into(&self, testcase: &mut Testcase<BytesInput>) -> Result<(), Error> {
        let id = self
            .testcases
            .iter()
            .position(|t| std::ptr::eq(t.as_ptr(), testcase))
            .map(CorpusId::from)
            .ok_or_else(|| Error::key_not_found("Testcase not found in corpus"))?;

        let data = self.read_object(&self.make_key(id.0))?;

        testcase.set_input(BytesInput::new(data));
        Ok(())
    }

    fn store_input_from(&self, testcase: &Testcase<BytesInput>) -> Result<(), Error> {
        let id = self
            .testcases
            .iter()
            .position(|t| std::ptr::eq(t.as_ptr(), testcase))
            .map(CorpusId::from)
            .ok_or_else(|| Error::key_not_found("Testcase not found in corpus"))?;

        let input_bytes = testcase
            .input()
            .as_ref()
            .ok_or_else(|| Error::illegal_state("Cannot store testcase with None input"))?
            .bytes();

        self.write_object(&self.make_key(id.0), input_bytes)?;
        Ok(())
    }
}
