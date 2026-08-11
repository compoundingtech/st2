# Claude channel packaging boundary

This page defines the optional work needed to make `st2 claude-mcp` a managed
Claude Code channel plugin. It is a decision aid, not build authority.

## Decision

Do not package the plugin yet. The maintained private-fleet path owns the st2
binary and its startup PTY. It can accept the provider's exact direct-server
warning once per Claude process. This is bounded startup control, not screen
classification for each message.

The plugin is worth building when a deployment requires zero-interaction
startup, a centrally managed allowlist, or distribution of the channel metadata
independently from st2. Until then it adds a marketplace, installation, and
organization-policy surface without changing the MCP transport or durable inbox
contract.

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
the channel. The plugin manifest selects the MCP server named by its `channels`
entry; the command-line selector names the plugin and marketplace.

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

This policy is needed only when the deployment uses managed settings. A host
with no managed policy can accept a locally declared server through the
development selector. A host whose managed policy disables channels rejects the
development selector too.

The provider documents these rules under
[Enterprise controls](https://code.claude.com/docs/en/channels#enterprise-controls)
and
[Research preview](https://code.claude.com/docs/en/channels#research-preview).

## Direct-server acceptance boundary

The current maintained path starts the locally built and pinned st2 MCP server
with this selector:

```text
--dangerously-load-development-channels server:<server-name>
```

Claude asks for one confirmation on every process start. The launcher may send
Return only after it recognizes the exact warning and exact expected selector.
It must not answer a workspace trust question or another startup prompt on that
basis. After this startup gate, native DING uses only MCP notifications and no
terminal input.

This path is not permission to run arbitrary downloaded channel servers. If the
server is no longer a locally owned st2 artifact, or if the deployment requires
no startup confirmation, build and validate the plugin path above.
