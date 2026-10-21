# Legacy Web File Manager

这里保存原 Web 文件管理业务，包含 MySQL schema、用户与会话、文件上传下载与分享、页面资源及路由实验。代码仅供后续实现参考，默认不参与主项目构建。

它依赖 MySQL client、nlohmann/json、旧页面资源路径及原运行环境。通用 HTTP 层已经移除原 `/upload` 分段回调特判，因此该旧应用不属于当前回归测试或兼容性保证范围；如需恢复运行，应在 legacy 内独立适配，不要把业务逻辑重新放回 TCP/HTTP 基础库。
