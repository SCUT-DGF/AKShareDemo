<template>
  <ReportGenerator title="Generate Weekly Report" apiUrl="http://localhost:8000/api/weekly-reports/" />
</template>

<script>
import ReportGenerator from './ReportGenerator.vue';

export default {
  components: {
    ReportGenerator
  }
};
</script>
