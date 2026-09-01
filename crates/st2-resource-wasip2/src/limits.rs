use wasmtime::{Error, ResourceLimiter};

use crate::{LimitKind, RuntimeConfig};

pub(crate) struct InvocationLimits {
    memory_bytes: usize,
    table_elements: usize,
    instances: usize,
    tables: usize,
    memories: usize,
    exceeded: Option<LimitKind>,
}

impl InvocationLimits {
    pub(crate) fn new(config: &RuntimeConfig) -> Self {
        Self {
            memory_bytes: config.max_memory_bytes,
            table_elements: config.max_table_elements,
            instances: config.max_instances,
            tables: config.max_tables,
            memories: config.max_memories,
            exceeded: None,
        }
    }

    pub(crate) fn exceeded(&self) -> Option<LimitKind> {
        self.exceeded
    }
}

impl ResourceLimiter for InvocationLimits {
    fn memory_growing(
        &mut self,
        _current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> Result<bool, Error> {
        if desired > self.memory_bytes || maximum.is_some_and(|maximum| desired > maximum) {
            self.exceeded = Some(LimitKind::Memory);
            return Err(Error::msg("guest linear-memory limit exceeded"));
        }
        Ok(true)
    }

    fn table_growing(
        &mut self,
        _current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> Result<bool, Error> {
        if desired > self.table_elements || maximum.is_some_and(|maximum| desired > maximum) {
            self.exceeded = Some(LimitKind::Table);
            return Err(Error::msg("guest table-element limit exceeded"));
        }
        Ok(true)
    }

    fn instances(&self) -> usize {
        self.instances
    }

    fn tables(&self) -> usize {
        self.tables
    }

    fn memories(&self) -> usize {
        self.memories
    }
}
