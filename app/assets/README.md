# 继承的视觉资产

当前样式沿用旧项目实际 Streamlit 工作台的蓝白配色、布局和字体。`theme.css`移除了旧推荐列表专用规则；新页面只绑定用户行为业务。

`plex.woff2`与`noto.woff2`复用旧项目缓存的IBM Plex Sans及Noto Sans SC上游字体，通过`scripts/subset-ui-fonts.py`为本项目页面文字重新生成子集，保留原SIL OFL许可证和`fonts.json`摘要。字体注入本地data URL，不请求CDN。原字体来源为Google Fonts官方仓库的`ofl/ibmplexsans`与`ofl/notosanssc`；内部字体名称沿用Retail Numerals与Retail Chinese。
