import VueRouter from 'vue-router'
import Vue from 'vue'
import CompanyInfomation from '@/views/CompanyInfomation'
import StockPriceTrend from '@/views/StockPriceTrend'
import dailyReport from '@/views/dailyReport'
import weeklyReport from '@/views/weeklyReport'
import AHInfomation from '@/views/AHInfomation'
import limitUpStock from '@/views/limitUpStock'
Vue.use(VueRouter)
const router = new VueRouter({
    routes: [
        { path: '/', redirect: '/companyinfo' },
        { path: '/companyinfo', component: CompanyInfomation },
        { path: '/stockpricetrend', component: StockPriceTrend },
        { path: '/weeklyReport', component: weeklyReport },
        { path: '/AHInfomation', component: AHInfomation },
        { path: '/dailyReport', component: dailyReport },
        { path: '/limitUpStock', component: limitUpStock },
    ]
})
export default router