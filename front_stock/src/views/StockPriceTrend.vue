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

      <el-button type="primary" class="formInput" @click="GetImg"
        >确定</el-button
      >
    </el-form>
    <hr />
    <img :src="imageUrl" alt="" />
    <div>
      <canvas class="stockCanvas" height="600" width="600"></canvas>
    </div>
  </div>
</template>

<script>
import request from "@/utils/request";
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
      imageUrl: "",
    };
  },
  methods: {
    async GetImg() {
      const res = await request.get("/c336372b0efeb5d6e5c501dcdd71d81.png", {
        responseType: "blob",
      });
      this.imageUrl = URL.createObjectURL(res);
    },
    async ChangeStock(query) {
      this.options = [];
      if (query !== "") {
        const res1 = await request.get("/sh_a_stocks.json");
        const list1 = res1.map((obj) => {
          return obj.代码 + "-" + obj.名称;
        });
        list1.forEach((element) => {
          if (element.includes(query)) {
            const obj = { value: element.toString() };
            this.options.push(obj);
          }
        });
        const res2 = await request.get("/sz_a_stocks.json");
        const list2 = res2.map((obj) => {
          return obj.代码 + "-" + obj.名称;
        });
        list2.forEach((element) => {
          if (element.includes(query)) {
            const obj = { value: element.toString() };
            this.options.push(obj);
          }
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