const { defineConfig } = require('@vue/cli-service')
module.exports = defineConfig({
  transpileDependencies: true
})
module.exports = {
  devServer: {
    proxy: {
      '/api': {
        target: 'http://127.0.0.1:8000',  // 目标后端服务器的地址
        changeOrigin: true,               // 是否改变请求头中的Host
//        pathRewrite: { '^/api': '' },     // 可选：重写路径，将 `/api` 去掉
      },
    },
  },
};
