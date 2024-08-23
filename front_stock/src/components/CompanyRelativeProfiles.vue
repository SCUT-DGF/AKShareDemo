<!-- src/components/CompanyRelativeProfiles.vue -->
<template>
  <ReportGenerator title="Generate Company Relative Profiles" apiUrl="http://localhost:8000/api/company-relative/" />
</template>

<script>
import ReportGenerator from './ReportGenerator.vue';

export default {
  components: {
    ReportGenerator
  }
};
</script>
