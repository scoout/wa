# Bot PLTGU Muara Karang - Panduan Instalasi (Termux & OpenWrt)

Bot ini dirancang untuk berjalan 24 jam nonstop dengan fitur pemulihan koneksi otomatis.

## 🚀 Cara Instalasi di Termux (Android)

Jika Anda melihat error saat `npm install` (terutama pada modul `sharp`), ikuti langkah berikut:

1.  **Buka Termux** dan masuk ke folder bot.
2.  **Jalankan perintah Setup Khusus Termux**:
    ```bash
    npm run termux-setup
    ```
    *Perintah ini akan menginstall compiler (clang, make, python) dan library `libvips` yang dibutuhkan oleh modul image processing.*

3.  **Jalankan Bot**:
    ```bash
    npm start
    ```

## 🛠️ Cara Instalasi di OpenWrt (Router)

Pastikan router Anda memiliki sisa space (Overlay) minimal 200MB dan RAM yang cukup.

1.  **Install Node.js & Dependencies**:
    ```bash
    opkg update
    opkg install node node-npm python3 make gcc
    ```
2.  **Clone & Install**:
    ```bash
    npm install --production
    ```
3.  **Jalankan**:
    ```bash
    node server.ts
    ```

## 💡 Tips Handal 24 Jam
- Gunakan `pm2` untuk menjaga bot tetap menyala jika terjadi crash:
  `npm install -g pm2`
  `pm2 start server.ts --interpreter tsx` (untuk dev) atau `pm2 start server.ts` (untuk prod).
- Pastikan `GEMINI_API_KEY` sudah terpasang di environment variable.
