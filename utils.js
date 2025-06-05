const disallowedValues = [
  '[not provided]',
  'placeholder',
  '[[unknown]]',
  'not set',
  'not provided',
  'unknown',
  'undefined',
  'n/a'
];

const filterNullValuesFromObject = object =>
  Object
    .fromEntries(
      Object
        .entries(object)
        .filter(([_, v]) =>
          v !== null &&
          v !== '' &&
          typeof v !== 'undefined' &&
            // this logic seems odd, but with the more specific requirements I cannot say it is wrong or not, even though I think it should be && instead of || for dissalowedValues
          (typeof v !== 'string' || (!disallowedValues.includes(v.toLowerCase()) && !v.toLowerCase().includes('!$record')))));

const normalizePropertyName = key => key.toLowerCase().replace(/__c$/, '').replace(/^_+|_+$/g, '').replace(/_+/g, '_');

const goal = actions => {
  // logging only meeting related actions to avoid unnecessary noise
  const meetingActions = actions.filter(
    action => action.actionName === 'Meeting Created' || action.actionName === 'Meeting Updated'
  );
  if (meetingActions.length > 0) {
    console.log(meetingActions);
  } else {
    console.log('[goal] No Meeting related actions');
  }
};

module.exports = {
  filterNullValuesFromObject,
  normalizePropertyName,
  goal
};
