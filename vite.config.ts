import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// 회사 프로젝트 로컬 서버(3000/5173/8080 등)와 절대 겹치지 않는 전용 포트.
// 개발(HMR) 시에만 사용되며, 배포 빌드는 로컬 포트를 전혀 열지 않는다.
const DEV_PORT = 41730;
const HMR_PORT = 41731;

export default defineConfig({
  plugins: [react()],
  clearScreen: false,
  server: {
    port: DEV_PORT,
    strictPort: true,
    hmr: { port: HMR_PORT },
    watch: { ignored: ["**/src-tauri/**"] },
  },
  build: { target: "es2022" },
});
