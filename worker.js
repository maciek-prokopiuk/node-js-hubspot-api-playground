const hubspot = require('@hubspot/api-client');
const { queue } = require('async');
const _ = require('lodash');

const { filterNullValuesFromObject, goal } = require('./utils');
const Domain = require('./Domain');

const HS_LAST_MODIFIED_DATE = 'hs_lastmodifieddate';
const LAST_MODIFIED_DATE = 'lastmodifieddate';
const HS_MEETING_TITLE = 'hs_meeting_title';
const HS_CREATEDATE = 'hs_createdate';
const HS_MEETING_START_TIME = 'hs_meeting_start_time';
const EMAIL = 'email';

const ASCENDING = 'ASCENDING';
const POST = 'post';
const CONTACTS_TO_COMPANIES_BATCH_PATH = '/crm/v3/associations/CONTACTS/COMPANIES/batch/read';
const MEETINGS_SEARCH_PATH = '/crm/v3/objects/meetings/search';
const MEETINGS_TO_CONTACTS_BATCH_PATH = '/crm/v3/associations/MEETINGS/CONTACTS/batch/read';

const MEETING_CREATED = 'Meeting Created';
const MEETING_UPDATED = 'Meeting Updated';
const CONTACT_CREATED = 'Contact Created';
const CONTACT_UPDATED = 'Contact Updated';
const COMPANY_CREATED = 'Company Created';
const COMPANY_UPDATED = 'Company Updated';

const hubspotClient = new hubspot.Client({ accessToken: '' });
const propertyPrefix = 'hubspot__';
let expirationDate;

const withRetry = async (fn, options) => {
  const { maxRetries = 4, onRetry, context } = options;
  let lastError;

  for (let tryCount = 0; tryCount <= maxRetries; tryCount++) {
    try {
      return await fn();
    } catch (err) {
      lastError = err;
      if (onRetry) {
        await onRetry(err, tryCount, context);
      }
      if (tryCount < maxRetries) {
        const delay = 5000 * Math.pow(2, tryCount);
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }
  }
  throw lastError;
};

const handleRetry = async (err, tryCount, context) => {
  const { domain, hubId } = context;

  if ((err.response?.status === 401) || (new Date() > expirationDate)) {
    await refreshAccessToken(domain, hubId);
  } else if (err.response?.status === 429) {
    // rate limit exceeded, ideally we should add circuit braker, but since we have a retry backoff ignoring it should just be fine
  }
};

const generateLastModifiedDateFilter = (date, nowDate, propertyName = HS_LAST_MODIFIED_DATE) => {
  const EMPTY_FILTERS = {};
  const lastModifiedDateFilter = date ?
    {
      filters: [
        { propertyName, operator: 'GTE', value: `${date.valueOf()}` },
        { propertyName, operator: 'LTE', value: `${nowDate.valueOf()}` }
      ]
    } : EMPTY_FILTERS;

  return lastModifiedDateFilter;
};

const saveDomain = async domain => {
  // disable this for testing purposes
  return;

  domain.markModified('integrations.hubspot.accounts');
  await domain.save();
};

/**
 * Get access token from HubSpot
 */
const refreshAccessToken = async (domain, hubId, tryCount) => {
  const { HUBSPOT_CID, HUBSPOT_CS } = process.env;
  const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
  const { accessToken, refreshToken } = account;

  return hubspotClient.oauth.tokensApi
    .createToken('refresh_token', undefined, undefined, HUBSPOT_CID, HUBSPOT_CS, refreshToken)
    .then(async result => {
      const body = result.body ? result.body : result;

      const newAccessToken = body.accessToken;
      expirationDate = new Date(body.expiresIn * 1000 + new Date().getTime());

      hubspotClient.setAccessToken(newAccessToken);
      if (newAccessToken !== accessToken) {
        account.accessToken = newAccessToken;
      }

      return true;
    });
};

const hubspotDataFetcher = async (config) => {
  const {
    domain,
    hubId,
    q,
    lastPulledDatesProperty,
    searchFunction,
    searchProperties,
    lastModifiedPropertyName,
    processBatchFunction
  } = config;

  const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
  const lastPulledDate = new Date(account.lastPulledDates[lastPulledDatesProperty]);
  const now = new Date();

  let hasMore = true;
  const offsetObject = {};
  const limit = 100;

  while (hasMore) {
    const lastModifiedDate = offsetObject.lastModifiedDate || lastPulledDate;
    const lastModifiedDateFilter = generateLastModifiedDateFilter(lastModifiedDate, now, lastModifiedPropertyName);
    const searchObject = {
      filterGroups: [lastModifiedDateFilter],
      sorts: [{ propertyName: lastModifiedPropertyName, direction: ASCENDING }],
      properties: searchProperties,
      limit,
      after: offsetObject.after
    };

    const searchResult = await withRetry(
      () => searchFunction(searchObject),
      {
        onRetry: handleRetry,
        context: { domain, hubId }
      }
    );

    const results = searchResult.results || [];
    offsetObject.after = searchResult.paging?.next?.after ? parseInt(searchResult.paging.next.after) : null;

    if (results.length > 0) {
      await processBatchFunction(results, { domain, hubId, q, lastPulledDate });
    }

    if (!offsetObject.after) {
      hasMore = false;
    } else if (offsetObject.after >= 9900) {
      offsetObject.after = 0;
      if (results.length > 0) {
        const lastItemInBatch = results[results.length - 1];
        const lastModifiedDateValue = lastItemInBatch.properties?.[lastModifiedPropertyName] || lastItemInBatch.updatedAt;
        offsetObject.lastModifiedDate = new Date(lastModifiedDateValue).valueOf();
      } else {
        hasMore = false;
      }
    }
  }

  account.lastPulledDates[lastPulledDatesProperty] = now;
  await saveDomain(domain);
  return true;
};

const processCompanyBatch = async (companies, { q, lastPulledDate }) => {
  companies.forEach(company => {
    if (!company.properties) return;

    const actionTemplate = {
      includeInAnalytics: 0,
      companyProperties: {
        company_id: company.id,
        company_domain: company.properties.domain,
        company_industry: company.properties.industry
      }
    };

    const isCreated = !lastPulledDate || (new Date(company.createdAt) > lastPulledDate);

    q.push({
      actionName: isCreated ? COMPANY_CREATED : COMPANY_UPDATED,
      actionDate: new Date(isCreated ? company.createdAt : company.updatedAt) - 2000,
      ...actionTemplate
    });
  });
};

const processContactBatch = async (contacts, { q, lastPulledDate }) => {
  const contactIds = contacts.map(contact => contact.id);
  let companyAssociations = {};
  if (contactIds.length > 0) {
    const companyAssociationsResults = (await (await hubspotClient.apiRequest({
      method: POST,
      path: CONTACTS_TO_COMPANIES_BATCH_PATH,
      body: { inputs: contactIds.map(id => ({ id })) }
    })).json())?.results || [];

    companyAssociations = companyAssociationsResults.reduce((acc, result) => {
      if (result.from && result.to?.length > 0) {
        acc[result.from.id] = result.to[0].id;
      }
      return acc;
    }, {});
  }

  contacts.forEach(contact => {
    if (!contact.properties || !contact.properties.email) return;

    const companyId = companyAssociations[contact.id];
    const isCreated = new Date(contact.createdAt) > lastPulledDate;

    const contactProperties = {
      company_id: companyId,
      contact_name: ((contact.properties.firstname || '') + ' ' + (contact.properties.lastname || '')).trim(),
      contact_title: contact.properties.jobtitle,
      contact_source: contact.properties.hs_analytics_source,
      contact_status: contact.properties.hs_lead_status,
      contact_score: parseInt(contact.properties.hubspotscore) || 0
    };

    const actionTemplate = {
      includeInAnalytics: 0,
      identity: contact.properties.email,
      userProperties: filterNullValuesFromObject(contactProperties)
    };

    q.push({
      actionName: isCreated ? CONTACT_CREATED : CONTACT_UPDATED,
      actionDate: new Date(isCreated ? contact.createdAt : contact.updatedAt),
      ...actionTemplate
    });
  });
};

const processMeetingBatch = async (meetings, { domain, hubId, q, lastPulledDate }) => {
  if (meetings.length === 0) return;

  const meetingIds = meetings.map(meeting => meeting.id);
  const associationInputs = meetingIds.map(id => ({ id }));

  const associationResults = await withRetry(
    async () => {
      const response = await hubspotClient.apiRequest({
        method: 'POST',
        path: MEETINGS_TO_CONTACTS_BATCH_PATH,
        body: { inputs: associationInputs }
      });
      return response.json();
    },
    { onRetry: handleRetry, context: { domain, hubId } }
  );

  const contactAssociations = (associationResults?.results || []).reduce((acc, result) => {
    if (result.from && result.to?.length > 0) {
      const fromId = result.from.id;
      const toIds = result.to.map(assoc => assoc.id);
      acc[fromId] = [...(acc[fromId] || []), ...toIds];
    }
    return acc;
  }, {});

  const contactIdsToFetch = Object.values(contactAssociations).flat().filter(id => id);
  let contactEmailsMap = {};
  if (contactIdsToFetch.length > 0) {
    const uniqueContactIds = [...new Set(contactIdsToFetch)];
    const batchReadInputs = uniqueContactIds.map(id => ({ id }));
    const contactsBatchResponse = await hubspotClient.crm.contacts.batchApi.read({
      inputs: batchReadInputs,
      properties: [EMAIL]
    });

    if (contactsBatchResponse?.results) {
      contactsBatchResponse.results.forEach(contact => {
        if (contact.id && contact.properties?.email) {
          contactEmailsMap[contact.id] = contact.properties.email;
        }
      });
    }
  }

  meetings.forEach(meeting => {
    if (!meeting.properties) return;

    const associatedContactIds = contactAssociations[meeting.id] || [];
    const contactEmails = associatedContactIds.map(id => contactEmailsMap[id]).filter(Boolean);

    const createdAt = new Date(meeting.properties.hs_createdate);
    const updatedAt = new Date(meeting.properties.hs_lastmodifieddate);
    const isCreated = !lastPulledDate || createdAt > lastPulledDate;
    const actionDate = isCreated ? createdAt : updatedAt;

    const actionTemplate = {
      actionName: isCreated ? MEETING_CREATED : MEETING_UPDATED,
      actionDate: actionDate,
      meetingProperties: {
        meeting_id: meeting.id,
        meeting_title: meeting.properties[HS_MEETING_TITLE],
        hs_createdate: meeting.properties[HS_CREATEDATE],
        hs_lastmodifieddate: meeting.properties[HS_LAST_MODIFIED_DATE],
        hs_meeting_start_time: meeting.properties[HS_MEETING_START_TIME]
      },
      contactEmails: contactEmails,
    };

    actionTemplate.meetingProperties = filterNullValuesFromObject(actionTemplate.meetingProperties);
    q.push(actionTemplate);
  });
};

/**
 * Get recently modified companies as 100 companies per page
 */
const processCompanies = async (domain, hubId, q) => {
  return hubspotDataFetcher({
    domain,
    hubId,
    q,
    lastPulledDatesProperty: 'companies',
    searchFunction: (searchObject) => hubspotClient.crm.companies.searchApi.doSearch(searchObject),
    searchProperties: [
      'name', 'domain', 'country', 'industry', 'description', 'annualrevenue', 'numberofemployees', 'hs_lead_status'
    ],
    lastModifiedPropertyName: HS_LAST_MODIFIED_DATE,
    processBatchFunction: processCompanyBatch
  });
};

/**
 * Get recently modified contacts as 100 contacts per page
 */
const processContacts = async (domain, hubId, q) => {
  return hubspotDataFetcher({
    domain,
    hubId,
    q,
    lastPulledDatesProperty: 'contacts',
    searchFunction: (searchObject) => hubspotClient.crm.contacts.searchApi.doSearch(searchObject),
    searchProperties: [
      'firstname', 'lastname', 'jobtitle', EMAIL, 'hubspotscore', 'hs_lead_status', 'hs_analytics_source', 'hs_latest_source'
    ],
    lastModifiedPropertyName: LAST_MODIFIED_DATE,
    processBatchFunction: processContactBatch
  });
};

/**
 * Get recently modified meetings as 100 meetings per page
 */
const processMeetings = async (domain, hubId, q) => {
  const searchFunction = (searchObject) =>
    hubspotClient.apiRequest({
      method: 'POST',
      path: MEETINGS_SEARCH_PATH,
      body: searchObject
    }).then(res => res.json());

  return hubspotDataFetcher({
    domain,
    hubId,
    q,
    lastPulledDatesProperty: 'meetings',
    searchFunction,
    searchProperties: [
      HS_MEETING_TITLE,
      HS_CREATEDATE,
      HS_LAST_MODIFIED_DATE,
      HS_MEETING_START_TIME
    ],
    lastModifiedPropertyName: HS_LAST_MODIFIED_DATE,
    processBatchFunction: processMeetingBatch
  });
};

const createQueue = (domain, actions) => queue(async (action, callback) => {
  actions.push(action);

  if (actions.length > 2000) {
    console.log('inserting actions to database', { apiKey: domain.apiKey, count: actions.length });

    const copyOfActions = _.cloneDeep(actions);
    actions.splice(0, actions.length);

    goal(copyOfActions);
  }

  callback();
}, 100000000); // wtf ? :D

const drainQueue = async (domain, actions, q) => {
  if (q.length() > 0) await q.drain();

  if (actions.length > 0) {
    goal(actions);
  }

  return true;
};

const pullDataFromHubspot = async () => {
  console.log('start pulling data from HubSpot');

  const domain = await Domain.findOne({});

  for (const account of domain.integrations.hubspot.accounts) {
    console.log('start processing account');

    try {
      await refreshAccessToken(domain, account.hubId);
    } catch (err) {
      console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'refreshAccessToken' } });
    }

    const actions = [];
    const q = createQueue(domain, actions);

    try {
      await processContacts(domain, account.hubId, q);
      console.log('process contacts');
    } catch (err) {
      console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'processContacts', hubId: account.hubId } });
    }

    try {
      await processCompanies(domain, account.hubId, q);
      console.log('process companies');
    } catch (err) {
      console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'processCompanies', hubId: account.hubId } });
    }

    try {
      await processMeetings(domain, account.hubId, q);
      console.log('process meetings');
    } catch (err) {
      console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'processMeetings', hubId: account.hubId } });
    }

    try {
      await drainQueue(domain, actions, q);
      console.log('drain queue');
    } catch (err) {
      console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'drainQueue', hubId: account.hubId } });
    }

    await saveDomain(domain);

    console.log('finish processing account');
  }

  process.exit();
};

module.exports = pullDataFromHubspot;