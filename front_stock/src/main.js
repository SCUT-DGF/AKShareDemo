import Vue from 'vue'
import App from './App.vue'
import ElementUI from 'element-ui';
import 'element-ui/lib/theme-chalk/index.css';
import axios from 'axios';
import router from './router/index.js'

Vue.prototype.$axios = axios
Vue.config.productionTip = false
Vue.use(ElementUI);


new Vue({
  render: h => h(App),
  router
}).$mount('#app')
