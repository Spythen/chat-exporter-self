# chat-exporter-self

A universal Discord chat exporter built for **selfbot usage**, supporting servers, DMs, and group chats.

## 🚀 Installation

```bash
pip install git+https://github.com/Spythen/chat-exporter-self.git
```

## 🔌 Compatibility

Designed specifically for selfbot environments and works seamlessly with  
[discord.py-self](https://github.com/dolfies/discord.py-self)

## ✨ Features

- **Selfbot Integration:** Works seamlessly with selfbot environments and supports Direct Messages (DMs) and Group Chats (GCs).
- **🎨 Multi-Theme Support:** Includes Dark, Light, Ash, and Onyx themes. Persists user selections locally and features an intuitive visual picker (**🎨 Theme: Dropdown**).
- **📱 Phone-Optimized (Mobile Responsive):** Fully optimized mobile layouts. Puts header controls inline to save screen space, auto-scales replied-to reference messages, and hides heavy text file previews on mobile for a clean layout.
- **🖼️ Smart Lightbox Image Viewer:** Redesigned media popups:
  - Left-click to open popups.
  - PC: Click to zoom, hold-and-drag to pan, and right-click for options.
  - Mobile: Supports native multi-touch pinch-to-zoom.
  - Top-right close (`X`) button and native browser **Back button** interception (so pressing Back closes the image instead of closing the browser tab).
- **📂 Discord-Style Attachment Cards:** Redesigned file downloads with dark rounded container boxes, clean vector file icons, and native Discord-style filename links.

## ⚠️ Limitations

- Discord component embeds are **not supported yet**

## 📌 About This Fork

This project is based on [DiscordChatExporterPy](https://github.com/mahtoid/DiscordChatExporterPy) by mahtoid.

This fork introduces several improvements and fixes, including:
- Added support for DMs and Group Chats
- Adapted for selfbot environments
- Modern responsive layout and theme system
- Various bug fixes and enhancements

## 🙌 Credits

- Original project: **mahtoid**
- Fork & enhancements: **Spythen (DC: therealvelocity)**
