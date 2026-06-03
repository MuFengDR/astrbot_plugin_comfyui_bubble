# AstrBot Comfyui 泡泡版🫧 工作流插件

将 ComfyUI 工作流封装为 LLM 可调用的工具，也支持用户通过 `/comfyui` 命令直接执行工作流。**向 BOT 描述你的目的，或直接指定工作流和参数，最终将图片/视频/文本等产物发送给你。**

> 本插件基于原插件 [cjxzdzh/astrbot_plugin_comfyui](https://github.com/cjxzdzh/astrbot_plugin_comfyui) 修改扩展，增加 WebSocket 事件等待、命令式执行、多 ComfyUI 接口切换、工作流白名单等能力，并修复了一些原插件使用中的问题。

---


### 适用场景

- **文生图**：用文字描述，让 BOT 根据可用工作流生成图片
- **文生视频**：用文字描述，生成视频
- **图文改图**：发图 + 修改说明，让 BOT 按你的要求修改图片
- **命令直连工作流**：不经过 LLM 改写，直接用 `/comfyui 工作流 参数` 提交任务
- **多 ComfyUI 接口**：在多台 ComfyUI 机器之间切换，并为每个接口配置可用工作流

### 使用方式

你可以直接向 BOT 说出需求，也可以使用命令手动指定工作流。

LLM 自动调用时，BOT 会：

1. 自动检查当前有哪些可用工作流
2. 根据你的描述选择合适的工作流并调用
3. 通过 ComfyUI WebSocket 等待任务完成
4. 将生成的图片、视频或文本发送给你

命令手动调用时，用户输入的文本会原样传入工作流，不经过 LLM 改写。

### 使用示例

| 你说什么 | 说明 |
|----------|------|
| 帮我画一张猫猫动漫图 | 文生图，BOT 会选用文生图工作流并生成 |
| （引用一条带图的消息）帮我将黑丝改成白丝 | 图文改图，BOT 会取被引用的图并按说明修改 |
| 之前画的白丝图，脱了吧 | 引用之前 BOT 发的图，继续修改；BOT 会找到该图并调用改图工作流 |
| `/comfyui 1 nahida, cute` | 手动执行列表第 1 个工作流 |
| `/comfyui 图生视频_720p 图中女孩跳舞｜5` | 手动执行指定工作流，多个文本参数用 `|` 或 `｜` 分隔 |

---

## 目标功能（技术说明）

**任意**在 ComfyUI 上能跑通的工作流，只要把「需要由 LLM/用户传入」的**文本、图片、视频**入口，换成约定好的几类节点（Simple String、ETN_LoadImageBase64、VHS_LoadVideo），即可接入 AstrBot，由 LLM 自动注入参数并执行。

- **约定**：可注入的入口仅限上述三类节点；工作流里其他逻辑（模型、采样、ControlNet、多步推理等）一律保持原样。
- **流程**：在 ComfyUI 中设计好工作流 → 导出 API 格式 JSON → 按规范命名（见第四节）→ 上传到本插件并填写说明，即可被 LLM 选用并调用。

---

## 一、依赖说明

### 1.1 AstrBot 插件依赖（可选但推荐）

- **[astrbot_plugin_qq_tools](https://github.com/YUMU1658/astrbot_plugin_qq_tools)**（工具名均为 `qts_` 开头）
  - 提供 **`qts_get_recent_messages`**、**`qts_get_message_detail`** 等：获取最近消息列表及单条消息详情。
  - 当用户引用上一条消息中的图/视频（如「把这张图改成雨天」）时，LLM 可先调用 **`qts_get_recent_messages`** 找到对应消息，再用 **`qts_get_message_detail`** 取详情；若能取得媒体 URL 或可访问的本地路径，可传入 `comfyui_execute` 的 **`image_urls`**。**获取最近媒体信息时优先使用 qts_get_recent_messages。**

- **[astrbot_plugin_image_url_base64_to_mcp](https://github.com/Thetail001/astrbot_plugin_image_url_base64_to_mcp)**
  - 提供 LLM 工具 **`get_image_from_context`**：从对话上下文中获取用户发送的图片（URL 或 base64）。
  - 当用户说「改这张图」但当前消息里未带图、或平台未把图片注入到本插件可读的 message 时，LLM 可先调用 `get_image_from_context` 拿到图片 URL，再在 `comfyui_execute` 中传入 **`image_urls=[该 URL]`**，由本插件下载并转为 base64 注入工作流。
  - 不安装该插件也可使用本插件，但「引用之前消息里的图」进行改图时，需依赖该工具或上述 qts 工具才能稳定拿到图片。

### 1.2 Python 依赖

- 见 **`requirements.txt`**（如 httpx、aiofiles）。将本插件放入 AstrBot 的 `data/plugins/` 后，按 AstrBot 惯例安装依赖即可。

---

## 二、ComfyUI 端依赖（节点与插件）

本插件**只替换**工作流中以下三类节点的输入，其余节点不修改。设计工作流时请仅使用这些节点作为「可注入参数」的入口。

| 用途 | 节点 `class_type` | 输入键名 | 说明 | ComfyUI 插件来源 |
|------|--------------------|----------|------|------------------|
| **文本** | `Simple String` | `text` 或 `string` | 按顺序注入 `texts` 数组 | **[CG Use Everywhere](https://github.com/chrisgoringe/cg-use-everywhere)**（chrisgoringe） |
| **图片** | `ETN_LoadImageBase64` | `image` | 按顺序注入 base64 字符串（PNG） | **ComfyUI Nodes for External Tooling**（如 [comfyui-tooling-nodes](https://comfyai.run/documentation/ETN_LoadImageBase64) / Acly 等），界面显示为 "Load Image (Base64)" |
| **视频** | `VHS_LoadVideo` | `video` | 按顺序注入服务器上的视频文件名（如 .mp4） | **ComfyUI-VideoHelperSuite**（[Kosinkadink/ComfyUI-VideoHelperSuite](https://github.com/Kosinkadink/ComfyUI-VideoHelperSuite)），界面为 "Load Video" 等 |

- **Simple String**：来自 [CG Use Everywhere](https://github.com/chrisgoringe/cg-use-everywhere)，需安装该扩展。
- **ETN_LoadImageBase64**：非内置，需安装 External Tooling / Base64 相关自定义节点，用于接收 base64 图参与推理。
- **VHS_LoadVideo**：来自 VideoHelperSuite，需单独安装该扩展；若工作流不涉及视频可不安。

---

## 三、ComfyUI 工作流设计思路

1. **需要传入的图片**  
   - 在工作流中，凡是要由 LLM/用户传入的**图片**，不要用「Load Image（从路径）」等节点，改用 **ETN_LoadImageBase64**（Base64 节点）作为入口，由本插件在运行时按顺序注入 base64 数据。

2. **需要传入的文本**  
   - 凡是要由 LLM/用户传入的**文本**，使用 **Simple String** 节点作为前级：在需要文本的地方（如 CLIP 文本编码、提示词输入等），用 **Simple String** 的输出连过去，本插件会按顺序把 `texts` 数组注入到这些 Simple String 节点。

3. **顺序与数量**  
   - 工作流中第 1 个 Simple String 对应 `texts[0]`，第 2 个对应 `texts[1]`，以此类推；图片、视频同理。输入/输出数量不再写在文件名里，而是在工作流管理页为每个工作流单独配置。

4. **模型与路径**  
   - 工作流内用到的模型（如 ckpt、LoRA）、路径等，需在**运行本插件的 ComfyUI 环境**中存在，否则会报错（如 `value_not_in_list`）。不同机器需各自准备好相同名称或调整工作流中的模型名。

---

## 四、本插件使用思路

### 4.1 安装与配置

- 将本插件目录放入 AstrBot 的 **`data/plugins/`**（或通过插件市场安装）。  
- 在 AstrBot 配置中查看/设置：
  - **当前接口名称 / 当前接口 HTTP 地址**：仅用于只读展示，接口增删和切换请在 Management page 中完成，或发送 `/comfyui_port <接口名称>`。
  - **client_id**：ComfyUI 客户端 ID（可选，按需填写）。  
  - **websocket_wait_timeout_seconds**：等待 ComfyUI WebSocket 完成事件的最长时间，默认 900 秒。
- 若需工作流管理页：启用 **webui_enabled**，配置 **webui_host** / **webui_port**（如 `http://127.0.0.1:6187`），重载插件后浏览器访问该地址。
- 工作流管理页可以动态添加 ComfyUI 接口、HTTP 地址，以及每个接口允许使用的工作流白名单。白名单为空表示该接口允许全部工作流。

### 4.2 上传工作流与参数配置

1. **在 ComfyUI 中导出工作流**  
   - 在 ComfyUI 中配置好工作流节点并确保可运行后，选择菜单 **文件 → 导出（API）**，会生成一个 `.json` 文件。  
   - 将该 JSON 文件重命名为易读名称，例如：`改图.json`、`图生视频_720p.json`。文件名不再承载输入/输出数量。

2. **上传、填写说明并配置参数**  
   - 打开本插件的**工作流管理页**（见 4.1），点击上传，选择重命名后的 `.json` 文件上传。  
   - 在管理页中为该工作流输入说明文字，并配置输入/输出的文本、图片、视频数量与强/弱校验模式。  
   - 也可将 `.json` 直接放到 **`data/plugin_data/astrbot_plugin_comfyui_bubble/workflows/`** 目录下，再在管理页中编辑说明。

3. **参数配置规则（核心）**  
   工作流管理页中，每个工作流都可以配置输入和输出的 **文本 / 图片 / 视频**：

   - 数量留空：接受任意数量。  
   - 数量填 `0`：表示该类型不应输入或不应输出。  
   - 强校验：输入数量必须等于设置值；输出数量必须不少于设置值，少于则报错，超过则只发送前 N 个。  
   - 弱校验：输入超过设置值时只取前 N 个，少于则按实际数量传入；输出超过设置值时只发送前 N 个，少于则按实际数量发送。  
   - 文件名中的 `+文本N+图片N` / `=文本N` 旧规则已经移除，不再解析；如需兼容旧文件，请在 WebUI 中手动配置显示名称和参数。

   **文件名与配置示例：**

   | 文件名 | WebUI 参数配置示例 |
   |--------|--------------------|
   | `改图.json` | 输入文本弱 1、输入图片强 1、输出图片弱 1 |
   | `双图参考改图.json` | 输入文本弱 1、输入图片强 2、输出图片弱 1 |
   | `文生图1比1.json` | 输入文本弱 1、输出图片弱 1 |
   | `手办化.json` | 输入图片强 1、输出图片弱 1 |
   | `图生视频_720p.json` | 输入文本弱 2、输入图片强 1、输出视频弱 1 |
   | `反推提示词.json` | 输入图片强 1、输出文本强 1 |

4. **填写说明（推荐）**  
   - 在工作流管理页为每个文件填写**说明**（或直接编辑 `data/plugin_data/astrbot_plugin_comfyui_bubble/workflow_meta.json`）。  
   - 说明会通过 `/comfyui list` 和 `comfyui_list_workflows` 返回，便于用户复制工作流名，也便于 LLM 选择合适工作流。  
   - 建议把输入/输出要求直接写进说明里，例如需要几段文本、几张图片、输出什么内容等。LLM 工具不会再额外拼接自动生成的参数摘要。
   - `workflow_meta.json` 中仍会保存说明，例如：
     ```json
     {
       "descriptions": {
         "改图.json": "根据文本修改图片。flux2_klein9B 模型。",
         "双图参考改图.json": "用于将图片1根据图片2和指定的文本要求进行修改。传入的文本须为「根据图2的XX修改图1」之类。"
       }
     }
     ```

### 4.3 LLM 使用流程建议

1. 调用 **`comfyui_list_workflows`**：获取可用工作流列表与说明，参数要求以说明内容为准。  
2. 按用户意图选择工作流，若需要图但当前消息无图：先调用 **`get_image_from_context`**（需安装 astrbot_plugin_image_url_base64_to_mcp）获取 URL。  
3. 调用 **`comfyui_execute`**：传入 `workflow_name`、`texts`、`image_urls`（可选）、`videos`（可选）；图片可从当前消息自动提取，或通过 `image_urls` 传入 URL/本地路径（仅限插件数据目录内）。  
4. 调用 **`comfyui_query_wait`**：通过 ComfyUI WebSocket 等待任务完成；结果若为图片/视频，插件会自动发送到当前会话。  
5. 可选：**`comfyui_status`** 查看队列状态。

### 4.4 手动命令调用

如果不希望 prompt 被 LLM 改写，可以直接使用 `/comfyui` 命令执行工作流。

```text
/comfyui list
/comfyui <工作流名称或编号> <文本1>|<文本2>|...
/comfyui upload
```

- `/comfyui list`：列出当前 ComfyUI 接口允许使用的工作流。列表编号按当前过滤结果生成，可直接用编号执行。
- `/comfyui <工作流名称或编号> ...`：提交工作流并等待 WebSocket 完成事件，用户输入文本会原样进入 Simple String 节点。
- 多段文本使用半角 `|` 或全角 `｜` 分隔；如果工作流只接收 1 段文本，剩余内容会作为一整段文本传入。
- 图片可直接随命令发送，也支持引用 Telegram 图片消息；若适配器能解析引用原图，插件会把引用图片作为 `图片N` 输入。
- `/comfyui upload`：回复一条 `.json` 工作流文件后上传到工作流目录。

示例：

```text
/comfyui 1 nahida(genshin impact), cute
/comfyui 图生视频_720p 图中女孩跳舞|5
/comfyui 图片编辑高清修复 图中角色做一个比心动作
```

### 4.5 多 ComfyUI 接口与工作流白名单

插件支持动态配置多个 ComfyUI 接口，适合同时接入高性能主机、低功耗主机、远程机器等场景。

- 在工作流管理页的“ComfyUI 接口配置”中添加接口，并填写接口名称和 HTTP 地址。
- 每个接口可以勾选允许使用的工作流；未勾选的工作流不会出现在 `/comfyui list` 和 `comfyui_list_workflows` 中，也不能被执行。
- 某个接口允许全部工作流时，可以开启“允许全部工作流”。
- 使用 `/comfyui_port` 查看当前接口和可用接口。
- 使用 `/comfyui_port <接口名称>` 切换当前全局接口，切换结果会保存，重启后仍生效。

示例：

```text
/comfyui_port
/comfyui_port 高性能机
```

### 4.6 WebSocket 等待机制

提交任务后，插件不再依赖固定 sleep 或估算轮询时间，而是连接 ComfyUI 官方 WebSocket：

```text
ws://<ComfyUI地址>/ws?clientId=<client_id>
```

当收到 `executing` 且 `node = null` 的完成事件后，插件立即读取 `/history/{prompt_id}` 获取输出。这样短任务可以更快返回，长任务也只保持 WebSocket 等待，不会频繁轮询 `/queue`。

如果 WebSocket 不可用，插件会返回明确的配置/网络错误。常见原因包括 ComfyUI 地址不可达、反代不支持 WebSocket、代理规则错误、`client_id` 不一致等。

### 4.7 安全与目录

- **本地图片路径**：`comfyui_execute` 的 `image_urls` 若传入本地路径，仅允许以下根目录之下：**插件数据目录**（`data/plugin_data/astrbot_plugin_comfyui_bubble/`）、**`data/agent/comfyui/input/`**、**`data/temp/`**（平台/适配器存放用户上传图的临时目录，避免「图在 temp 不被认可」导致 images=0）。建议使用绝对路径。禁止 `../` 路径穿越。
- **占位符与持久化路径**：发送 ComfyUI 生成的图片/视频时，插件会将其另存到 `data/agent/comfyui/input/`，并通过占位符或自动队列发送给用户。聊天内容中不会主动暴露本地图片/视频路径。
- **清理本地缓存**：工作流管理页提供「清理本地缓存」按钮，可删除 `data/agent/comfyui/input/` 与插件 `tmp/` 下的文件，防止占用过多磁盘空间。  
- **base64 不传入 LLM**：优先使用 URL 或本地路径；若图片来源工具返回占位符（如 `base64://ASTRBOT_PLUGIN_CACHE_PENDING`），请将占位符传入 `image_urls`，不要将原始 base64 填入工具参数，以免 base64 进入 LLM 上下文。插件侧已对相关日志脱敏。  
- 插件数据目录：**`data/plugin_data/astrbot_plugin_comfyui_bubble/`**，其中 **`workflows/`** 存放工作流 JSON，**`workflow_meta.json`** 存放说明与参数配置。旧目录 `data/plugin_data/astrbot_plugin_comfyui_pro/` 不会自动迁移，请按需手动复制。

---

## 五、功能概览（简要）

| 能力 | 说明 |
|------|------|
| **comfyui_list_workflows** | 查询可用工作流及说明 |
| **comfyui_execute** | 提交工作流；支持 texts、image_urls（URL 或允许范围内的本地路径）、videos |
| **comfyui_query_wait** | 通过 WebSocket 等待任务完成；图片/视频由插件自动发送到会话 |
| **comfyui_status** | 查询 ComfyUI 队列状态（运行中/等待中数量） |
| **/comfyui** | 手动列出、上传、执行工作流，绕过 LLM prompt 改写 |
| **/comfyui_port** | 查看/切换当前 ComfyUI 接口 |
| **工作流管理页** | 上传/重命名/删除工作流 JSON，编辑说明，配置多接口与工作流白名单 |

---

## 六、注意事项小结

- 工作流中**仅** Simple String、ETN_LoadImageBase64、VHS_LoadVideo 会被本插件替换；其他节点请勿依赖「由本插件注入」的文本/图/视频。  
- 工作流文件名不再承载参数；请在工作流管理页配置输入/输出文本、图片、视频数量及强/弱校验模式。  
- 数量留空表示接受任意数量；数量填 `0` 表示该类型不应输入或不应输出。  
- 旧的 `+文本N+图片N` / `=文本N` 文件名规则已移除，不再解析。  
- 推荐安装 [astrbot_plugin_image_url_base64_to_mcp](https://github.com/Thetail001/astrbot_plugin_image_url_base64_to_mcp)，以便在「用户引用之前消息的图」时通过 `get_image_from_context` + `image_urls` 完成改图。
- 本版本在原插件基础上修复了一些稳定性、等待机制、媒体发送与参数处理方面的问题。

---
