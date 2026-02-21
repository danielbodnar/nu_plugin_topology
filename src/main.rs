use nu_plugin::{serve_plugin, MsgPackSerializer};
use nu_plugin_topology::TopologyPlugin;

fn main() {
    serve_plugin(&TopologyPlugin, MsgPackSerializer {})
}
