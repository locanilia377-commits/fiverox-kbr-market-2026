// Commands retain their original payload until the server settles the operation.
// This queue contains no persisted credentials or business data outside memory.
export function createWriteQueue({invoke, validate, getView, makeId, now = () => performance.now(), onChange = () => {}}) {
  const operations = new Map();
  let tail = Promise.resolve();
  const scopeOf = (name, args) => name === 'crmCreateLead' ? 'new-lead' : name === 'crmItemSave' ? args[2]?.leadId : args[0];
  function run(name, args, key) {
    const fingerprint = JSON.stringify([name, args]), view = getView(), scope = scopeOf(name, args);
    const prior = operations.get(key);
    if (prior && prior.fingerprint !== fingerprint) {
      return Promise.reject(new Error('Сначала повторите исходную неподтверждённую запись. Её поля и номер операции сохранены.'));
    }
    const other = [...operations.values()].find(op => op.scope === scope && op.key !== key);
    if (other) return Promise.reject(new Error('По этой карточке уже есть неподтверждённая запись. Сначала завершите её.'));
    if (prior?.promise) return prior.promise;
    const wasUncertain = prior?.state === 'uncertain';
    const operation = prior || {key, name, args: structuredClone(args), fingerprint, id: makeId(), view, scope};
    operation.state = 'queued'; operations.set(key, operation); onChange();
    const queuedAt = now();
    const promise = tail.then(async () => {
      if (view !== getView() || operation.view !== view) throw Object.assign(new Error('Запрос отменён.'), {abandoned: true});
      operation.state = 'running'; operation.queueMs = Math.max(0, now() - queuedAt); onChange();
      const result = await invoke(name, ...operation.args, operation.id);
      if (view !== getView()) throw Object.assign(new Error('Запрос отменён.'), {abandoned: true});
      // A 200 response alone does not confirm a valid CRM write.
      validate(name, operation.args, result);
      operation.milliseconds = Math.max(0, now() - queuedAt);
      if (operations.get(key) === operation) operations.delete(key); onChange();
      return {result, milliseconds: operation.milliseconds, queueMs: operation.queueMs};
    }).catch(error => {
      if (view !== getView() || error.abandoned) { if (operations.get(key) === operation) operations.delete(key); throw error; }
      if (error.crmDefiniteRejection && !wasUncertain) { if (operations.get(key) === operation) operations.delete(key); }
      else { operation.state = 'uncertain'; operation.error = error.message; }
      onChange(); throw error;
    }).finally(() => { operation.promise = null; });
    operation.promise = promise;
    tail = promise.catch(() => {});
    return promise;
  }
  function clear() { operations.clear(); tail = Promise.resolve(); }
  return {operations, run, clear, busy: id => [...operations.values()].some(op => op.scope === id)};
}

export function validateWriteResult(name, args, result, stages) {
  const bad = () => { throw Object.assign(new Error('Ответ о записи не подтверждён. Повторите исходную операцию: её номер и поля сохранены.'), {crmWriteUncertain: true}); };
  const card = (value, id, allowDerivedFields = false) => {
    const keys = ['stage','manager','due','next','last','note','amount','reason'];
    const fields = value?.fields ?? (allowDerivedFields && keys.map(key => String(value?.[key] ?? '')));
    if (!value || value.id !== id || !Array.isArray(fields) || fields.length !== 8 ||
        !fields.every(field => typeof field === 'string') || !stages.includes(value.stage) || fields[0] !== value.stage) bad();
    if (keys.some((key, index) => String(value[key] ?? '') !== fields[index])) bad();
  };
  if (name === 'crmMoveRead') {
    if (!result || typeof result.conflict !== 'boolean' || !stages.includes(result.stage)) bad();
    card(result.card, args[0]);
    if (!result.conflict && result.stage !== args[2]) bad();
  } else if (name === 'crmSaveRead' || name === 'crmRecordRead') {
    card(result, args[0]);
    if (name === 'crmRecordRead' && (!Array.isArray(result.history) || !Array.isArray(result.audit))) bad();
  } else if (name === 'crmCreateLead') {
    if (!/^L-[a-f0-9-]+$/i.test(result?.id || '')) bad();
    card(result, result.id, true);
  } else if (name === 'crmItemSave') {
    const [kind, original, requested] = args;
    if (!result || typeof result.id !== 'string' || !result.id || (original?.id && result.id !== original.id) ||
        result.leadId !== requested.leadId || result.status !== requested.status || !result.updatedAt || !result.updatedBy ||
        !['activity','sale','transfer'].includes(kind)) bad();
  } else bad();
  return result;
}
