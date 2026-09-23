import { useState } from 'react';
import { Pressable, SafeAreaView, StyleSheet, Text, View } from 'react-native';

const tabs = ['Now', 'Chat', 'Control', 'Fleet'] as const;
type Tab = (typeof tabs)[number];

export default function App() {
  const [active, setActive] = useState<Tab>('Now');

  return (
    <SafeAreaView style={styles.page}>
      <View style={styles.header}>
        <Text style={styles.brand}>Smalltalk</Text>
        <Text style={styles.status}>Starter shell · no live data</Text>
      </View>
      <View style={styles.content}>
        <Text style={styles.title}>Hello, Smalltalk.</Text>
        <Text style={styles.section}>{active}</Text>
        <Text style={styles.explanation}>
          The {active} screen is ready for the product build. No connection or actions yet.
        </Text>
      </View>
      <View accessibilityRole="tablist" style={styles.tabs}>
        {tabs.map((tab) => (
          <Pressable
            key={tab}
            accessibilityRole="tab"
            accessibilityState={{ selected: active === tab }}
            onPress={() => setActive(tab)}
            style={[styles.tab, active === tab && styles.activeTab]}
          >
            <Text style={[styles.tabText, active === tab && styles.activeTabText]}>{tab}</Text>
          </Pressable>
        ))}
      </View>
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  page: { flex: 1, backgroundColor: '#101923' },
  header: { paddingHorizontal: 24, paddingTop: 28, paddingBottom: 20 },
  brand: { color: '#f3f7fa', fontSize: 24, fontWeight: '700' },
  status: { color: '#9eb0bd', marginTop: 5 },
  content: { flex: 1, justifyContent: 'center', paddingHorizontal: 28 },
  title: { color: '#f3f7fa', fontSize: 30, fontWeight: '700' },
  section: { color: '#67d6c5', fontSize: 20, fontWeight: '600', marginTop: 24 },
  explanation: { color: '#b8c7d0', fontSize: 16, lineHeight: 24, marginTop: 8 },
  tabs: { flexDirection: 'row', borderTopWidth: 1, borderTopColor: '#344651' },
  tab: { flex: 1, alignItems: 'center', paddingVertical: 18 },
  activeTab: { borderTopWidth: 3, borderTopColor: '#67d6c5' },
  tabText: { color: '#a9bac5', fontSize: 13, fontWeight: '600' },
  activeTabText: { color: '#f3f7fa' }
});
