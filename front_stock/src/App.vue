<template>
  <div class="app">
    <NavigationBar></NavigationBar>
    <router-view></router-view>
    <WeeklyReport />
    <DailyReport />
    <CheckDailyUp />
    <MergeZtA />
    <UpAhReport />
    <CompanyBasicProfile />
    <CompanyRelativeProfiles />
  </div>
</template>

<script>
import NavigationBar from "@/components/NavigationBar";
import WeeklyReport from '@/components/WeeklyReport.vue';
import DailyReport from '@/components/DailyReport.vue';
import CheckDailyUp from '@/components/CheckDailyUp.vue';
import MergeZtA from '@/components/MergeZtA.vue';
import UpAhReport from '@/components/UpAhReport.vue';
import CompanyBasicProfile from '@/components/CompanyBasicProfile.vue';
import CompanyRelativeProfiles from '@/components/CompanyRelativeProfiles.vue';

export default {
  name: "App",
  components: {
    NavigationBar,
    WeeklyReport,
    DailyReport,
    CheckDailyUp,
    MergeZtA,
    UpAhReport,
    CompanyBasicProfile,
    CompanyRelativeProfiles
  },
};
</script>

<style>
.app {
  padding-top: 60px;
}
</style>
