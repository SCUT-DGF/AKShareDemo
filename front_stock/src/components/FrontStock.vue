<template>
  <div>
    <div class="header">
      <div class="title">基于机器学习的股票评估预测系统</div>
      <ul class="nav">
        <li><a href="#">首页</a></li>
        <li><a href="#">风险评估</a></li>
        <li><a href="#">收益预测</a></li>
        <li><a href="#">策略模拟回测</a></li>
        <li><a href="#">系统后台</a></li>
      </ul>
    </div>
    <hr />
    <el-form class="timeForm">
      <div class="block">
        <span class="demonstration">选择起始日期</span>
        <el-date-picker
          v-model="datevalue"
          type="daterange"
          range-separator="至"
          start-placeholder="开始日期"
          end-placeholder="结束日期"
        >
        </el-date-picker>
      </div>

      <div>
        <span class="demonstration">选择股票</span>
        <el-select
          v-model="value"
          filterable
          clearable
          remote
          :remote-method="ChangeStock"
          :loading="loading"
          placeholder="请选择"
          @change="ChangeStock"
        >
          <el-option
            v-for="item in options"
            :key="item.value"
            :label="item.value"
            :value="item.value"
          >
          </el-option>
        </el-select>
      </div>
      <el-button type="primary" class="formInput" @click="fn">确定</el-button>
    </el-form>
    <hr />
    <div>
      <canvas class="stockCanvas" height="600" width="600"></canvas>
    </div>
  </div>
</template>

<script>
export default {
  name: "FrontStock",
  data() {
    return {
      pickerOptions: {
        disabledDate(time) {
          return time.getTime() > Date.now();
        },
      },
      datevalue: "",
      stockvalue: "",
      options: [
        {
          value: "111-华为",
        },
        {
          value: "111-xiaomi",
        },
        {
          value: "111-123",
        },
        {
          value: "111-333",
        },
        {
          value: "111-14124",
        },
      ],
      value: "",
    };
  },
  methods: {
    ChangeStock(query) {
      console.log(query);
      if (query !== "") {
        this.loading = true;
        this.$axios({
          url: "#",
          params: {
            stockinfo: query,
          },
        });
        setTimeout(() => {
          this.loading = false;
          this.options = this.list.filter((item) => {
            return item.label.toLowerCase().indexOf(query.toLowerCase()) > -1;
          });
        }, 200);
      } else {
        this.options = [];
      }
    },
  },
  fn() {
    console.log(this.datevalue[0].toLocaleDateString());
    console.log(this.datevalue[1].toLocaleDateString());
    console.log(this.value);
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

.header {
  height: 60px;
  display: flex;
  justify-content: center;
  align-items: center;
  background-color: #f2f2f2;
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

.nav li a:visited {
  color: black;
}

.nav li a:hover {
  color: turquoise;
}

.timeForm {
  height: 60px;
  display: flex;
  justify-content: space-around;
  align-items: center;
}

.formInput {
  padding: 5px 10px;
}
.demonstration {
  margin-right: 10px;
}
</style>