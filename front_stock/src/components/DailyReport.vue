<template>
  <ReportGenerator title="Generate Daily Report" apiUrl="http://localhost:8000/api/daily-reports/" />
</template>

<script>
import ReportGenerator from './ReportGenerator.vue';

export default {
  components: {
    ReportGenerator
  }
};
</script>
