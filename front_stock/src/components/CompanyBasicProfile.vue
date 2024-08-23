<!-- src/components/CompanyBasicProfile.vue -->
<template>
  <ReportGenerator title="Generate Company Basic Profile" apiUrl="http://localhost:8000/api/company-basic/" />
</template>

<script>
import ReportGenerator from './ReportGenerator.vue';

export default {
  components: {
    ReportGenerator
  }
};
</script>
