<template>
  <div>
    <h3>{{ title }}</h3>
    <form @submit.prevent="fetchReport">
      <div>
        <label for="date">Date (YYYYMMDD):</label>
        <input v-model="date" type="text" id="date" placeholder="Enter date in YYYYMMDD format" required />
      </div>
      <button type="submit">Generate Report</button>
    </form>
    <p v-if="message" :class="{ success: isSuccess, error: !isSuccess }">{{ message }}</p>
  </div>
</template>

<script>
import axios from 'axios';

export default {
  props: {
    title: {
      type: String,
      required: true
    },
    apiUrl: {
      type: String,
      required: true
    }
  },
  data() {
    return {
      date: '',
      message: '',
      isSuccess: false
    };
  },
  methods: {
    async fetchReport() {
      try {
        const response = await axios.get(this.apiUrl, {
          params: { date: this.date }
        });
        this.isSuccess = response.data.status === 'success';
        this.message = response.data.message;
      } catch (error) {
        this.isSuccess = false;
        this.message = error.response ? error.response.data.message : 'An error occurred';
      }
    }
  }
};
</script>

<style scoped>
.success {
  color: green;
}
.error {
  color: red;
}
</style>
