<template>
  <div class="app">
    <NavigationBar></NavigationBar>
    <router-view></router-view>
  </div>
</template>

<script>
import NavigationBar from "@/components/NavigationBar";
export default {
  name: "App",
  components: {
    NavigationBar,
  },
};
</script>

<style>
.app {
  padding-top: 60px;
}
</style>
