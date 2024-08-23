<template>
  <ReportGenerator title="Check Daily Up Interface" apiUrl="http://localhost:8000/api/check-daily-up/" />
</template>

<script>
import ReportGenerator from './ReportGenerator.vue';

export default {
  components: {
    ReportGenerator
  }
};
</script>
