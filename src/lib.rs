pub mod algo;

#[cfg(feature = "plugin")]
pub mod commands;

#[cfg(feature = "plugin")]
use nu_plugin::{Plugin, PluginCommand};

#[cfg(feature = "plugin")]
pub struct TopologyPlugin;

#[cfg(feature = "plugin")]
impl Plugin for TopologyPlugin {
    fn version(&self) -> String {
        env!("CARGO_PKG_VERSION").into()
    }

    fn commands(&self) -> Vec<Box<dyn PluginCommand<Plugin = Self>>> {
        vec![
            Box::new(commands::Sample),
            Box::new(commands::Fingerprint),
            Box::new(commands::Analyze),
            Box::new(commands::Classify),
            Box::new(commands::GenerateTaxonomy),
            Box::new(commands::Tags),
            Box::new(commands::Topics),
            Box::new(commands::Dedup),
            Box::new(commands::Organize),
            Box::new(commands::Similarity),
        ]
    }
}
