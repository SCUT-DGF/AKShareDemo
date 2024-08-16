<template>
  <div class="header">
    <div class="title">基于机器学习的股票评估预测系统</div>
    <ul class="nav">
      <li><router-link to="/companyinfo">公司信息</router-link></li>
      <li><router-link to="/dailyReport">每日报表</router-link></li>
      <li><router-link to="/weeklyReport">每周报表</router-link></li>
      <li><router-link to="/AHInfomation">AH股信息</router-link></li>
      <li><router-link to="/limitUpStock">涨停股票</router-link></li>
      <li><router-link to="/stockpricetrend">股价预测</router-link></li>
    </ul>
  </div>
</template>
<script>
export default {
  name: "NavigationBar",
};
</script>

<style scpoed>
* {
  text-decoration: none;
  list-style: none;
  margin: 0;
  padding: 0;
}
.header {
  height: 60px;
  width: 100%;
  display: flex;
  justify-content: center;
  align-items: center;
  background-color: #f2f2f2;
  position: fixed;
  top: 0;
  z-index: 1;
}
.title {
  font-size: 25px;
  margin-right: 20px;
}
.nav {
  display: flex;
}
.nav li {
  margin-right: 10px;
  color: black;
  font-size: 15px;
}

.nav li a {
  color: black;
}
.nav li a:hover {
  color: turquoise;
}
.nav li a:focus {
  color: turquoise;
}
</style>