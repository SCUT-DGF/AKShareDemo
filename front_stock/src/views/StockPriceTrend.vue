<template>
  <div>
    <hr />
    <el-form class="timeForm" :rules="formRules">
      <div class="block">
        <span class="demonstration">选择日期</span>
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
          placeholder="请选择"
          @focus="CleanStock"
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

      <el-row class="inputdate">
        <el-col :span="8">
          <span class="demonstration">选择预测天数</span></el-col
        >
        <el-col :span="16">
          <el-input placeholder="请输入预测天数" v-model="input" clearable>
          </el-input>
        </el-col>
      </el-row>

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
  name: "StockPriceTrend",
  data() {
    return {
      pickerOptions: {
        disabledDate(time) {
          return time.getTime() > Date.now();
        },
      },
      datevalue: "",
      stockvalue: "",
      options: [],
      value: "",
      input: "",
      formRules: {
        datevalue: [{ required: true, message: "请输入", trigger: blur }],
      },
    };
  },
  methods: {
    ChangeStock(query) {
      this.options = [];
      if (query !== "") {
        fetch("/sh_a_stocks.json")
          .then((res) => {
            return res.json();
          })
          .then((data) => {
            const list = data.map((obj) => {
              return obj.代码 + "-" + obj.名称;
            });
            list.forEach((element) => {
              if (element.includes(query)) {
                const obj = { value: element.toString() };
                this.options.push(obj);
              }
            });
          });
        fetch("/sz_a_stocks.json")
          .then((res) => {
            return res.json();
          })
          .then((data) => {
            const list = data.map((obj) => {
              return obj.代码 + "-" + obj.名称;
            });
            list.forEach((element) => {
              if (element.includes(query)) {
                const obj = { value: element.toString() };
                this.options.push(obj);
              }
            });
          });
      } else {
        this.options = [];
      }
    },
    CleanStock() {
      this.options = [];
    },
    fn() {
      console.log(this.datevalue[0].toLocaleDateString());
      console.log(this.datevalue[1].toLocaleDateString());
      console.log(this.value);
      console.log(this.input);
    },
  },
};
</script>

<style scoped>
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
.inputdate {
  display: flex;
  justify-content: space-around;
  align-items: center;
}
</style>