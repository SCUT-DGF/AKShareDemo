<!-- src/components/UpAhReport.vue -->
<template>
  <ReportGenerator title="Generate Up A-H Report Data" apiUrl="http://localhost:8000/api/up-ah-report/" />
</template>

<script>
import ReportGenerator from './ReportGenerator.vue';

export default {
  components: {
    ReportGenerator
  }
};
</script>
