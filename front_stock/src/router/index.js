import VueRouter from 'vue-router'
import Vue from 'vue'
import CompanyInfomation from '@/views/CompanyInfomation'
import StockPriceTrend from '@/views/StockPriceTrend'
Vue.use(VueRouter)
const router = new VueRouter({
    routes: [
        { path: '/', redirect: '/companyinfo' },
        { path: '/companyinfo', component: CompanyInfomation },
        { path: '/stockpricetrend', component: StockPriceTrend },
    ]
})
export default router