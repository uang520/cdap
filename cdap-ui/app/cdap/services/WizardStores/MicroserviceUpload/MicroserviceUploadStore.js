/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License s
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
import {combineReducers, createStore} from 'redux';
import MicroserviceUploadActions from 'services/WizardStores/MicroserviceUpload/MicroserviceUploadActions';
import MicroserviceUploadWizardConfig from 'services/WizardConfigs/MicroserviceUploadWizardConfig';
import {getArtifactNameAndVersion, defaultAction, requiredFieldsCompleted} from 'services/helpers';
import head from 'lodash/head';
import cloneDeep from 'lodash/cloneDeep';
import isEmpty from 'lodash/isEmpty';
import shortid from 'shortid';
import T from 'i18n-react';

// Defaults
// Disabled means that the step tab is not clickable at all
// Readonly means that the step is clickable, but all the contents are non-editable
const defaultState = {
  __complete: false,
  __skipped: false,
  __error: false,
  __disabled: false,
  __readOnly: false
};

const defaultGeneralState = Object.assign({
  instanceName: '',
  description: '',
  version: 1,
  defaultMicroservicePlugins: [],
  defaultMicroserviceOptions: [],
  microserviceOption: '',
  showNewMicroserviceTextbox: false,
  newMicroserviceName: ''
}, defaultState);

const defaultJarState = Object.assign({}, {
  contents: '',
  fileMetadataObj: {}
}, defaultState);

const defaultJsonState = Object.assign({}, {
  contents: '',
  properties: {},
  artifactExtends: '',
  artifactPlugins: []
}, defaultState);

const defaultConfigureState = Object.assign({
  instances: 1,
  vcores: 1,
  memory: 512,
  ethreshold: 100
}, defaultState, { __complete: true });

const defaultEndpointsState = Object.assign({
  fetch: 100,
  in: [{
    property: '',
    uniqueId : shortid.generate()
  }],
  out: [{
    property: '',
    uniqueId : shortid.generate()
  }],
}, defaultState, { __complete: true });

const defaultPropertiesState = Object.assign({
  keyValues : {
    pairs : [{
      key : '',
      value : '',
      uniqueId : shortid.generate()
    }]
  }
}, defaultState, { __complete: true });

const defaultInitialState = {
  general: defaultGeneralState,
  uploadjar: defaultJarState,
  uploadjson: defaultJsonState,
  configure: defaultConfigureState,
  endpoints: defaultEndpointsState,
  properties: defaultPropertiesState
};

const generalStepRequiredFields = head(
  MicroserviceUploadWizardConfig
    .steps
    .filter(step => step.id === 'general')
  ).requiredFields;
const configureStepRequiredFields = head(
  MicroserviceUploadWizardConfig
    .steps
    .filter(step => step.id === 'configure')
  ).requiredFields;

const uploadMicroserviceLabel = 'Upload new microservice';

const onErrorHandler = (reducerId, stateCopy, action) => {
  stateCopy = Object.assign({}, stateCopy);
  if (action.payload.id === reducerId) {
    stateCopy.__error = action.payload.error;
  }
  return stateCopy;
};
const onSuccessHandler = (reducerId, stateCopy, action) => {
  stateCopy = Object.assign({}, stateCopy, action);
  if (action.payload.id === 'general') {
    stateCopy.__complete = action.payload.res;
  }
  return stateCopy;
};

// Reducers
const general = (state = defaultGeneralState, action = defaultAction) => {
  let stateCopy = {};
  let showNewMicroserviceTextbox;
  let defaultMicroserviceOptions = [];

  switch (action.type) {
    case MicroserviceUploadActions.setInstanceName:
      stateCopy = Object.assign({}, state, {
        instanceName: action.payload.instanceName
      });
      break;
    case MicroserviceUploadActions.setDescription:
      stateCopy = Object.assign({}, state, {
        description: action.payload.description
      });
      break;
    case MicroserviceUploadActions.setVersion:
      stateCopy = Object.assign({}, state, {
        version: action.payload.version
      });
      break;
    case MicroserviceUploadActions.setMicroserviceOption:
      if (action.payload.microserviceOption === uploadMicroserviceLabel) {
        showNewMicroserviceTextbox = true;
      } else {
        showNewMicroserviceTextbox = false;
      }
      stateCopy = Object.assign({}, state, {
        microserviceOption: action.payload.microserviceOption,
        showNewMicroserviceTextbox
      });
      break;
    case MicroserviceUploadActions.setDefaultMicroservicePlugins:
      defaultMicroserviceOptions = action.payload.plugins.map(option => option.name);
      defaultMicroserviceOptions.push(uploadMicroserviceLabel);
      if (defaultMicroserviceOptions.length === 1) {
        showNewMicroserviceTextbox = true;
      } else {
        showNewMicroserviceTextbox = false;
      }
      if (state.microserviceOption === '') {
        stateCopy = Object.assign({}, state, {
          microserviceOption: defaultMicroserviceOptions[0]
        });
      }
      stateCopy = Object.assign({}, state, stateCopy, {
        defaultMicroservicePlugins: action.payload.plugins,
        defaultMicroserviceOptions,
        showNewMicroserviceTextbox
      });
      break;
    case MicroserviceUploadActions.setNewMicroserviceName:
      stateCopy = Object.assign({}, state, {
        newMicroserviceName: action.payload.newMicroserviceName
      });
      break;
    case MicroserviceUploadActions.setMicroserviceArtifact:
      if (isEmpty(action.payload.artifact)) {
        stateCopy = Object.assign({}, state, {
          __readOnly: true
        });
      } else {
        stateCopy = Object.assign({}, state);
      }
      break;
    case MicroserviceUploadActions.onError:
      return onErrorHandler('general', Object.assign({}, state), action);
    case MicroserviceUploadActions.onSuccess:
      return onSuccessHandler('general', Object.assign({}, state), action);
    case MicroserviceUploadActions.onReset:
      return defaultGeneralState;
    default:
      return state;
  }
  return Object.assign({}, stateCopy, {
    __complete: requiredFieldsCompleted(stateCopy, generalStepRequiredFields),
    __error: action.payload.error || false
  });
};
const uploadjar = (state = defaultJarState, action = defaultAction) => {
  let stateCopy, fileMetadataObj;
  switch (action.type) {
    case MicroserviceUploadActions.setFilePath:
      if (!action.payload.file.name.endsWith('.jar')) {
        return Object.assign({}, state, {
          __error: T.translate('features.Wizard.MicroserviceUpload.Step2.errorMessage')
        });
      }
      fileMetadataObj = getArtifactNameAndVersion(action.payload.file.name.split('.jar')[0]);
      stateCopy = Object.assign({}, state, {
        contents: action.payload.file,
        fileMetadataObj,
        __complete: true
      });
      break;
    case MicroserviceUploadActions.setMicroserviceOption:
      if (action.payload.microserviceOption === uploadMicroserviceLabel) {
        stateCopy = Object.assign({}, state, {
          __disabled: false
        });
      } else {
        stateCopy = Object.assign({}, state, {
          __disabled: true
        });
      }
      break;
    case MicroserviceUploadActions.setDefaultMicroservicePlugins:
      if (action.payload.plugins.length > 0) {
        stateCopy = Object.assign({}, state, {
          __disabled: true
        });
      } else {
        stateCopy = Object.assign({}, state);
      }
      break;
    case MicroserviceUploadActions.setMicroserviceArtifact:
      if (isEmpty(action.payload.artifact)) {
        stateCopy = Object.assign({}, state, {
          __disabled: true
        });
      } else {
        stateCopy = Object.assign({}, state);
      }
      break;
    case MicroserviceUploadActions.onError:
      return onErrorHandler('uploadjar', Object.assign({}, state), action);
    case MicroserviceUploadActions.onSuccess:
      return onSuccessHandler('uploadjar', Object.assign({}, state), action);
    case MicroserviceUploadActions.onReset:
      return defaultJarState;
    default:
      return state;
  }

  return Object.assign({}, stateCopy, {
    __error: action.payload.error || false
  });
};
const uploadjson = (state = defaultJsonState, action = defaultAction) => {
  let stateCopy;
  let pluginProperties,
      artifactExtends,
      artifactPlugins,
      artifactJson;
  switch (action.type) {
    case MicroserviceUploadActions.setJson:
      artifactJson = action.payload.json;
      try {
        artifactJson = JSON.parse(artifactJson);
      } catch (e) {
        return Object.assign({}, state, {
          __error: T.translate('features.Wizard.MicroserviceUpload.Step3.errorMessage')
        });
      }
      if (!artifactJson.parents) {
        return Object.assign({}, state, {
          __error: T.translate('features.Wizard.MicroserviceUpload.Step3.errorMessageParentArtifacts')
        });
      }
      pluginProperties = artifactJson.properties;
      artifactExtends = artifactJson.parents.reduce( (prev, curr) => `${prev}/${curr}`);
      artifactPlugins = artifactJson.plugins || [];
      stateCopy = Object.assign({}, state, {
        properties: pluginProperties,
        artifactExtends,
        artifactPlugins,
        contents: action.payload.jsonFile,
        __complete: true
      });
      break;
    case MicroserviceUploadActions.setMicroserviceOption:
      if (action.payload.microserviceOption === uploadMicroserviceLabel) {
        stateCopy = Object.assign({}, state, {
          __disabled: false
        });
      } else {
        stateCopy = Object.assign({}, state, {
          __disabled: true
        });
      }
      break;
    case MicroserviceUploadActions.setDefaultMicroservicePlugins:
      if (action.payload.plugins.length > 0) {
        stateCopy = Object.assign({}, state, {
          __disabled: true
        });
      } else {
        stateCopy = Object.assign({}, state);
      }
      break;
    case MicroserviceUploadActions.setMicroserviceArtifact:
      if (isEmpty(action.payload.artifact)) {
        stateCopy = Object.assign({}, state, {
          __disabled: true
        });
      } else {
        stateCopy = Object.assign({}, state);
      }
      break;
    case MicroserviceUploadActions.onError:
      return onErrorHandler('uploadjson', Object.assign({}, state), action);
    case MicroserviceUploadActions.onSuccess:
      return onSuccessHandler('uploadjson', Object.assign({}, state), action);
    case MicroserviceUploadActions.onReset:
      return defaultJsonState;
    default:
      return state;
  }

  return Object.assign({}, stateCopy, {
    __error: action.payload.error || false
  });
};
const configure = (state = defaultConfigureState, action = defaultAction) => {
  let stateCopy;
  switch (action.type) {
    case MicroserviceUploadActions.setInstances:
      stateCopy = Object.assign({}, state, {
        instances: action.payload.instances
      });
      break;
    case MicroserviceUploadActions.setVCores:
      stateCopy = Object.assign({}, state, {
        vcores: action.payload.vcores
      });
      break;
    case MicroserviceUploadActions.setMemory:
      stateCopy = Object.assign({}, state, {
        memory: action.payload.memory
      });
      break;
    case MicroserviceUploadActions.setThreshold:
      stateCopy = Object.assign({}, state, {
        ethreshold: action.payload.ethreshold
      });
      break;
    case MicroserviceUploadActions.setMicroserviceArtifact:
      if (isEmpty(action.payload.artifact)) {
        stateCopy = Object.assign({}, state, {
          __disabled: true
        });
      } else {
        stateCopy = Object.assign({}, state);
      }
      break;
    case MicroserviceUploadActions.onError:
      return onErrorHandler('configure', Object.assign({}, state), action);
    case MicroserviceUploadActions.onSuccess:
      return onSuccessHandler('configure', Object.assign({}, state), action);
    case MicroserviceUploadActions.onReset:
      return defaultConfigureState;
    default:
      return state;
  }
  return Object.assign({}, stateCopy, {
    __complete: requiredFieldsCompleted(stateCopy, configureStepRequiredFields),
    __error: action.payload.error || false
  });
};

const endpoints = (state = defaultEndpointsState, action = defaultAction) => {
  let stateCopy;
  switch (action.type) {
    case MicroserviceUploadActions.setFetchSize:
      stateCopy = Object.assign({}, state, {
        fetch: action.payload.fetchSize
      });
      break;
    case MicroserviceUploadActions.setInboundQueues:
      stateCopy = Object.assign({}, state, {
        in: cloneDeep(action.payload.inboundQueues)
      });
      break;
    case MicroserviceUploadActions.setOutboundQueues:
      stateCopy = Object.assign({}, state, {
        out: cloneDeep(action.payload.outboundQueues)
      });
      break;
    case MicroserviceUploadActions.setMicroserviceArtifact:
      if (isEmpty(action.payload.artifact)) {
        stateCopy = Object.assign({}, state, {
          __disabled: true
        });
      } else {
        stateCopy = Object.assign({}, state);
      }
      break;
    case MicroserviceUploadActions.onError:
      return onErrorHandler('endpoints', Object.assign({}, state), action);
    case MicroserviceUploadActions.onSuccess:
      return onSuccessHandler('endpoints', Object.assign({}, state), action);
    case MicroserviceUploadActions.onReset:
      return defaultEndpointsState;
    default:
      return state;
  }
  return Object.assign({}, stateCopy, {
    __error: action.payload.error || false
  });
};

const properties = (state = defaultPropertiesState, action = defaultAction) => {
  let stateCopy;
  switch (action.type) {
    case MicroserviceUploadActions.setProperties:
      stateCopy = Object.assign({}, state, {
        keyValues: action.payload.keyValues
      });
      break;
    case MicroserviceUploadActions.setMicroserviceArtifact:
      if (isEmpty(action.payload.artifact)) {
        stateCopy = Object.assign({}, state, {
          __disabled: true
        });
      } else {
        stateCopy = Object.assign({}, state);
      }
      break;
    case MicroserviceUploadActions.onError:
      return onErrorHandler('properties', Object.assign({}, state), action);
    case MicroserviceUploadActions.onSuccess:
      return onSuccessHandler('properties', Object.assign({}, state), action);
    case MicroserviceUploadActions.onReset:
      return defaultPropertiesState;
    default:
      return state;
  }
  return Object.assign({}, stateCopy, {
    __error: action.payload.error || false
  });
};

// Store
const createStoreWrapper = () => {
  return createStore(
    combineReducers({
      general,
      uploadjar,
      uploadjson,
      configure,
      endpoints,
      properties
    }),
    defaultInitialState
  );
};

const MicroserviceUploadStore = createStoreWrapper();
export default MicroserviceUploadStore;
export {createStoreWrapper};
