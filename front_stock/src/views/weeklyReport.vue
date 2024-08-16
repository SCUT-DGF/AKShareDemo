<template>
  <div>
    <hr>
    <div class="weeklyheader">
    <ul class="weeklynav">
      <li><a href="#" @click="change3">上涨三个交易日的股票</a></li>
      <li><a href="#" @click="change4">上涨四个交易日的股票</a></li>
      <li><a href="#" @click="change5">上涨五个交易日的股票</a></li>
    </ul>
  </div>
    <hr>
    <el-table class="eltable"
      :data="
        tableData.slice((currentPage - 1) * pageSize, currentPage * pageSize)
      "
      border
      style="width: 100%"
    >
      <el-table-column
        prop="代码"
        label="代码"
        align="center"
      >
      </el-table-column>
      <el-table-column prop="名称" label="名称" align="center">
      </el-table-column>
    </el-table>
    <div class="block" style="margin-top: 15px">
      <el-pagination
        align="center"
        @size-change="handleSizeChange"
        @current-change="handleCurrentChange"
        :current-page="currentPage"
        :page-sizes="5"
        :page-size="pageSize"
        layout="total, sizes, prev, pager, next, jumper"
        :total="tableData.length"
      >
      </el-pagination>
    </div>
  </div>
</template>

<script>
import request from "@/utils/request";
export default {
  name: "weeklyReport",
  data() {
    return {
      currentPage: 1, // 当前页码
      pageSize: 10, // 每页的数据条数
      tableData: [],
    };
  },
  async created() {
    const res = await request.get("/weekly_report_up_3_days_20240730.json");
    this.tableData = eval(res);
  },
  methods: {
    //每页条数改变时触发 选择一页显示多少行
    handleSizeChange(val) {
      this.currentPage = 1;
      this.pageSize = val;
    },
    //当前页改变时触发 跳转其他页
    handleCurrentChange(val) {
      this.currentPage = val;
    },
    async change3(){
      const res = await request.get("/weekly_report_up_3_days_20240730.json");
      this.tableData = eval(res);
    },
    async change4(){
      const res = await request.get("/weekly_report_up_4_days_20240730.json");
      this.tableData = eval(res);
    },
    async change5(){
      const res = await request.get("/weekly_report_up_5_days_20240730.json");
      this.tableData = eval(res);
    }
  },
};
</script>

<style scoped>
* {
  text-decoration: none;
  list-style: none;
  margin: 0;
  padding: 0;
}
.weeklyheader {
  height: 60px;
  width: 100%;
  display: flex;
  justify-content: center;
  align-items: center;
  background-color: #f2f2f2;
  position: fixed;
  top: 60px;
  z-index: 1;
}
.weeklynav {
  display: flex;
}
.weeklynav li {
  margin-right: 10px;
  color: black;
  font-size: 15px;
}

.weeklynav li a {
  color: black;
}
.weeklynav li a:hover {
  color: turquoise;
}
.weeklynav li a:focus {
  color: turquoise;
}
.eltable{
  padding-top: 60px;
}
</style>