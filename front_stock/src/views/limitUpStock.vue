<template>
  <div>
    <hr />
    <el-table
      :data="
        tableData.slice((currentPage - 1) * pageSize, currentPage * pageSize)
      "
      border
      style="width: 100%"
    >
      <el-table-column
        prop="公司名称"
        label="公司名称"
        align="center"
      >
      </el-table-column>
      <el-table-column prop="总股本" label="总股本" align="center">
      </el-table-column>
      <el-table-column
        prop="股票简称"
        label="股票简称"
        align="center"
      >
      </el-table-column>
      <el-table-column
        prop="股票代码"
        label="股票代码"
        align="center"
      >
      </el-table-column>
      <el-table-column prop="今日开盘股价" label="今日开盘股价" align="center">
      </el-table-column>
      <el-table-column
        prop="今日收盘股价"
        label="今日收盘股价"
        align="center"
      >
      </el-table-column>
      <el-table-column
        prop="今日开盘总市值"
        label="今日开盘总市值"
        align="center"
      >
      </el-table-column>
      <el-table-column prop="今日收盘总市值" label="今日收盘总市值" align="center" width="80">
      </el-table-column>
      <el-table-column prop="今日涨跌" label="今日涨跌" align="center">
      </el-table-column>
      <el-table-column
        prop="今日涨跌幅"
        label="今日涨跌幅"
        align="center"
      >
      </el-table-column>
      <el-table-column
        prop="今日收盘市盈率"
        label="今日收盘市盈率"
        align="center"
      >
      </el-table-column>
      <el-table-column
        prop="所属区域"
        label="所属区域"
        align="center"
        show-overflow-tooltip
      >
      </el-table-column>
      <el-table-column
        prop="主营业务"
        label="主营业务"
        align="center"
        show-overflow-tooltip
      >
      </el-table-column>
      <el-table-column
        prop="经营范围"
        label="经营范围"
        align="center"
        show-overflow-tooltip
      >
      </el-table-column>
      <el-table-column
        prop="公司简介"
        label="公司简介"
        align="center"
        show-overflow-tooltip
      >
      </el-table-column>
      <el-table-column
        prop="发行量"
        label="发行量"
        align="center"
      >
      </el-table-column>
      <el-table-column
        prop="发行价格"
        label="发行价格"
        align="center"
      >
      </el-table-column>
      <el-table-column
        prop="发行日期"
        label="发行日期"
        align="center"
      >
      </el-table-column>
      <el-table-column
        prop="注册资本"
        label="注册资本"
        align="center"
      >
      </el-table-column>
      <el-table-column
        prop="成立日期"
        label="成立日期"
        align="center"
      >
      </el-table-column>
      <el-table-column
        prop="近三年净利润"
        label="近三年净利润"
        align="center"
      >
      </el-table-column>
      <el-table-column
        prop="近三年营业收入"
        label="近三年营业收入"
        align="center"
      >
      </el-table-column>
      <el-table-column
        prop="近三年资产负债率"
        label="近三年资产负债率"
        align="center"
      >
      </el-table-column>

      <el-table-column
        prop="所属行业"
        label="所属行业"
        align="center"
      >
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
  name: "CompanyInfomation",
  data() {
    return {
      currentPage: 1, // 当前页码
      pageSize: 10, // 每页的数据条数
      tableData: [],
    };
  },
  async created() {
    const res = await request.get("/merge_zt_a_data_20240812.json");
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
</style>
<style lang="css"> .el-tooltip__popper{ max-width:50% } </style>
