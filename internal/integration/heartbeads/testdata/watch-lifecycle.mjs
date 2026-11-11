import assert from 'node:assert/strict';

// The barrier holds real HTTP requests before forwarding to the imported Plug
// process. It never substitutes snapshots, refresh(), timers or watch callbacks.
export async function verifyWatchLifecycle({ adapter, parentAdapter, binding, armHelloGate, setFault, receipts, pause, until, save, check }) {
  const cases = [];
  async function run(label, implementation, { outcome = 'fresh', cancelFirst = false, parent = false } = {}) {
    const callStart = receipts.calls.length, responseStart = receipts.responses.length;
    const store = new implementation.ProjectionStore(new implementation.PlugClient(binding));
    const values = [[], []], errors = [[], []], stops = [];
    let first, second;
    const observation = { label, outcome, parent, cancelFirst, values, errors };
    cases.push(observation);
    try {
      setFault('none');
      const seeded = await store.refresh();
      assert.equal(seeded.stats.total, 6);
      assert.equal(Object.keys(seeded.projects).length, 2);
      observation.seedGeneration = seeded.freshness.generated_at;
      first = armHelloGate();
      for (let i = 0; i < 2; i++) {
        stops.push(store.watch(
          snapshot => values[i].push(structuredClone(snapshot)),
          error => errors[i].push({ name: error.name, message: error.message }),
          { intervalMs: 5 },
        ));
      }
      await until(() => first.request, label + ' first actual HTTP request');
      // Many poll deadlines pass while this one refresh remains unresolved.
      await pause(120);
      if (cancelFirst) { stops[0](); stops[0](); }
      const shared = store.refresh();
      assert.equal(shared, store.refreshing);
      // Attach the error handler before releasing an authority-fault request.
      const sharedOutcome = shared.then(value => ({ value }), error => ({ error }));
      second = armHelloGate();
      setFault(outcome === 'stale' ? 'outage' : outcome === 'error' ? 'principal' : 'none');
      first.release();
      await until(() => second.request, label + ' next refresh after first outcome');
      await pause(60);
      observation.firstRequest = first.request;
      observation.secondRequest = second.request;
      const result = await sharedOutcome;
      if (outcome === 'error') {
        assert.ok(result.error instanceof implementation.AuthorityError);
        assert.equal(store.current, null);
        assert.deepEqual(values.map(list => list.length), [0, 0]);
        assert.deepEqual(errors.map(list => list.length), [1, 1]);
      } else {
        assert.ok(!result.error);
        assert.deepEqual(errors, [[], []]);
        if (parent) {
          assert.ok(values.every(list => list.length > 1), 'exact parent must reproduce callback backlog');
        } else {
          assert.deepEqual(values.map(list => list.length), cancelFirst ? [0, 1] : [1, 1]);
        }
        for (const list of values) for (const snapshot of list) {
          assert.equal(snapshot.stats.total, 6);
          assert.equal(snapshot.graphData.nodes.filter(node => node.id === 'SHARED').length, 2);
          assert.deepEqual(Object.keys(snapshot.projects).sort(), ['cody-ops', 'signals-v2']);
          assert.equal(snapshot.freshness.state, outcome);
          assert.equal(snapshot.freshness.generated_at, result.value.freshness.generated_at);
          if (outcome === 'stale') {
            // This controlled cache has no intervening refresh: unlike the
            // historical browser capture, this exact generation has a witness.
            assert.equal(snapshot.freshness.generated_at, seeded.freshness.generated_at);
            const withoutFreshness = ({ freshness, ...content }) => content;
            assert.deepEqual(withoutFreshness(snapshot), withoutFreshness(seeded));
          }
        }
      }
      observation.deliveriesBeforeStop = values.map(list => list.length);
      observation.errorsBeforeStop = errors.map(list => list.length);
      const completing = store.refresh();
      assert.equal(completing, store.refreshing);
      for (const stop of stops) { stop(); stop(); }
      setFault('none');
      second.release();
      const recovered = await completing;
      assert.equal(recovered.freshness.state, 'fresh');
      assert.equal(recovered.stats.total, 6);
      assert.deepEqual(values.map(list => list.length), observation.deliveriesBeforeStop);
      assert.deepEqual(errors.map(list => list.length), observation.errorsBeforeStop);
      const count = receipts.calls.length;
      await pause(60);
      assert.equal(receipts.calls.length, count, 'stopped watches must not start another refresh');
      observation.callsAfterStoppedRefreshSettled = receipts.calls.length - count;
      observation.recoveredGeneration = recovered.freshness.generated_at;
      observation.pass = true;
      check(label, { deliveries: observation.deliveriesBeforeStop, errors: observation.errorsBeforeStop, callsAfterStoppedRefreshSettled: observation.callsAfterStoppedRefreshSettled });
    } catch (error) {
      observation.failure = { message: error.message, stack: error.stack };
      throw error;
    } finally {
      for (const stop of stops) stop();
      setFault('none');
      first?.release(); second?.release();
      await store.refreshing?.catch(() => {});
      observation.calls = receipts.calls.slice(callStart);
      observation.responses = receipts.responses.slice(responseStart);
      await save('watch-lifecycle-results.json', { cases, boundary: 'actual imported SQLite/Plug HTTP; injected hello delays/503/principal faults; direct watch consumers, not browser rate qualification; in-flight refresh drains after stop' });
    }
  }
  await run('exact parent reproduces slow-refresh callback backlog', parentAdapter, { parent: true });
  await run('slow fresh refresh delivers once to each active watcher', adapter);
  await run('slow transport failure delivers one complete exact-generation stale snapshot per watcher', adapter, { outcome: 'stale' });
  await run('slow authority failure delivers one error and permits a subsequent refresh', adapter, { outcome: 'error' });
  await run('stopped watcher suppresses delivery while shared readers complete', adapter, { cancelFirst: true });
}
