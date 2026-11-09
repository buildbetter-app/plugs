// Runs inside the disposable browser. Observe the committed React tree and
// rendered desktop Activity panel without replacing callbacks or feed data.
export function observeDependencyRemoval(expected) {
  const samples = [], errors = [];
  let hostRoot;
  for (const element of document.querySelectorAll('*')) {
    const key = Object.keys(element).find(key => key.startsWith('__reactFiber$'));
    let fiber = key && element[key];
    if (!fiber) continue;
    while (fiber.return) fiber = fiber.return;
    if (fiber.stateNode?.current) { hostRoot = fiber.stateNode; break; }
  }
  if (!hostRoot) throw Error('committed React root missing');

  function capture() {
    let graph, activity, live;
    const pending = [hostRoot.current];
    while (pending.length) {
      const fiber = pending.pop();
      if (!fiber) continue;
      const props = fiber.memoizedProps;
      if (!graph && props?.onNodeRightClick && Array.isArray(props.nodes) && Array.isArray(props.links)) graph = props;
      if (!activity && Array.isArray(props?.events) && typeof props.isOpen === 'boolean' && props.onClose) activity = props;
      if (!live) {
        for (let hook = fiber.memoizedState; hook && typeof hook === 'object'; hook = hook.next) {
          const value = hook.memoizedState;
          if (value?.graphData && value?.freshness && value?.stats) { live = value; break; }
        }
      }
      if (fiber.sibling) pending.push(fiber.sibling);
      if (fiber.child) pending.push(fiber.child);
    }
    const removals = (activity?.events || []).filter(event => event.type === 'link-removed' && event.project === expected.project && event.meta?.targetLegacyId === expected.publicTarget);
    const links = (graph?.links || []).filter(link => link.type === 'blocks' && link.sourceKey === expected.sourceKey && link.targetKey === expected.targetKey);
    const aside = [...document.querySelectorAll('aside')].find(element => element.getClientRects().length && [...element.querySelectorAll('h2')].some(title => title.textContent.trim() === 'Activity'));
    const labelCount = aside ? [...aside.querySelectorAll('span')].filter(span => span.textContent === expected.label).length : null;
    samples.push({
      observedAt: Date.now(), graphPresent: Boolean(graph), activityPresent: Boolean(activity),
      freshness: live?.freshness || null,
      nodeCount: graph?.nodes.length ?? null,
      events: removals.map(({id, time, nodeId, project, detail, meta}) => ({id, time, nodeId, project, detail, meta})),
      desktopRemovalLabels: labelCount,
      renderedLinks: links.map(link => ({sourceKey: link.sourceKey, targetKey: link.targetKey, removeTime: link._removeTime ?? null})),
    });
  }
  const sample = () => { try { capture(); } catch (error) { errors.push({message: error.message, stack: error.stack}); } };
  sample();
  const timer = setInterval(sample, 75);
  return {
    read() { sample(); return {expected, samples, errors}; },
    stop() { clearInterval(timer); sample(); return {expected, samples, errors}; },
  };
}
