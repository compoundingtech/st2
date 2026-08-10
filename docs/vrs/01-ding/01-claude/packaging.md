# Claude channel packaging boundary

This page defines the work needed to make `st2 claude-mcp` an approved Claude
Code channel. It is a decision aid, not build authority.

## Plugin shape

The plugin source would live at `integrations/claude-channel/` in this
repository. Its root would contain two files:

```text
integrations/claude-channel/
├── .claude-plugin/plugin.json
└── .mcp.json
```

`.claude-plugin/plugin.json` would declare the stable plugin name and one
`channels` entry. The entry's `server` value would name the MCP server in
`.mcp.json`. `.mcp.json` would declare one standard-input MCP server whose
command is `st2` and whose arguments are `claude-mcp`. The installed st2 binary
would remain the implementation. The plugin would contain only provider
metadata and launch configuration.

Claude Code requires the channel server name in the manifest to match the MCP
server key. The plugin must pass `claude plugin validate --strict` before
distribution. See the provider's
[plugin reference](https://code.claude.com/docs/en/plugins-reference#channels).

## Private-fleet distribution

A private marketplace is a versioned catalog that points Claude Code at the
plugin source. Fleet setup would register that marketplace, install the plugin
at user or managed scope, and start each Claude agent with this selector:

```text
--channels plugin:st2-channel@<private-marketplace>
```

The selector is required for every session. Installation alone does not enable
the channel. A private marketplace also does not approve the channel during
the provider's research preview.

## Organization policy

An eligible organization can approve its private plugin without Anthropic
marketplace review. An Owner must enable Claude Code channels and set managed
policy like this:

```json
{
  "channelsEnabled": true,
  "allowedChannelPlugins": [
    { "marketplace": "<private-marketplace>", "plugin": "st2-channel" }
  ]
}
```

`allowedChannelPlugins` replaces the provider's default allowlist. The policy
must therefore include every channel plugin that the organization still wants
to permit. Users cannot override it.

This control is available to claude.ai Team and Enterprise organizations and
to qualifying Console organizations that deploy managed settings. Enabling it
in the claude.ai Admin console requires the Owner role. A personal Pro or Max
subscription cannot add a private plugin to this organization allowlist. Under
that subscription, the choices are Anthropic marketplace approval or a move to
an eligible organization and authentication path.

The provider documents these rules under
[Enterprise controls](https://code.claude.com/docs/en/channels#enterprise-controls)
and
[Research preview](https://code.claude.com/docs/en/channels#research-preview).

## Current acceptance boundary

The development-only override asks for interactive confirmation on every
Claude process start. It cannot support an unattended st2 agent. st2 must not
emit that flag in a maintained launcher.

The MCP server, watcher, poll backstop, and durable inbox boundary can be built
and tested without this package. Production launch acceptance waits for one of
the approved paths above.
