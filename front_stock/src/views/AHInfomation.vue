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
        prop="A股简称"
        label="A股简称"
        align="center"
      >
      </el-table-column>
      <el-table-column
        prop="A股股票代码"
        label="A股股票代码"
        align="center"
      >
      </el-table-column>
      <el-table-column prop="A股今日开盘股价" label="A股今日开盘股价" align="center">
      </el-table-column>
      <el-table-column
        prop="A股今日收盘股价"
        label="A股今日收盘股价"
        align="center"
      >
      </el-table-column>
      <el-table-column
        prop="A股今日开盘总市值"
        label="A股今日开盘总市值"
        align="center"
      >
      </el-table-column>
      <el-table-column prop="A股今日收盘总市值" label="A股今日收盘总市值" align="center">
      </el-table-column>
      <el-table-column prop="A股今日涨跌" label="A股今日涨跌" align="center">
      </el-table-column>
      <el-table-column
        prop="A股今日涨跌幅"
        label="A股今日涨跌幅"
        align="center"
      >
      </el-table-column>
      <el-table-column
        prop="A股今日收盘市盈率"
        label="A股今日收盘市盈率"
        align="center"
      >
      </el-table-column>
      <el-table-column
        prop="H股股票代码"
        label="H股股票代码"
        align="center"
      >
      </el-table-column>
      <el-table-column
        prop="H股今日开盘股价"
        label="H股今日开盘股价"
        align="center"
      >
      </el-table-column>
      <el-table-column
        prop="H股今日收盘股价"
        label="H股今日收盘股价"
        align="center"
      >
      </el-table-column>
      <el-table-column
        prop="H股今日开盘总市值"
        label="H股今日开盘总市值"
        align="center"
      >
      </el-table-column>
      <el-table-column
        prop="H股今日收盘总市值"
        label="H股今日收盘总市值"
        align="center"
      >
      </el-table-column>
      <el-table-column
        prop="H股今日涨跌"
        label="H股今日涨跌"
        align="center"
      >
      </el-table-column>
      <el-table-column
        prop="H股今日涨跌幅"
        label="H股今日涨跌幅"
        align="center"
      >
      </el-table-column>
      <el-table-column
        prop="H股今日收盘市盈率"
        label="H股今日收盘市盈率"
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
      // total: 9, // 总条数
      pageSize: 10, // 每页的数据条数
      tableData: [],
    };
  },
  async created() {
    const res = await request.get("/merge_up_ah_data_20240809.json");
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