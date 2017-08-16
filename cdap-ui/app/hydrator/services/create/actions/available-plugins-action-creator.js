/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

class PipelineAvailablePluginsActions {
  constructor(myPipelineApi, GLOBALS, $q) {
    this.api = myPipelineApi;
    this.GLOBALS = GLOBALS;
    this.$q = $q;
  }

  fetchPlugins(extensionsParams) {
    this.api.fetchExtensions(extensionsParams)
      .$promise
      .then((res) => {
        // filter out extensions
        const extensionsList = this.GLOBALS.pluginTypes[extensionsParams.pipelineType];

        const extensionMap = Object.keys(extensionsList).map((ext) => {
          return extensionsList[ext];
        });

        let supportedExtensions = res.filter((ext) => {
          return extensionMap.indexOf(ext) !== -1;
        });
        console.info('supported', supportedExtensions);

        this._fetchPlugins(extensionsParams, supportedExtensions);
      }, (err) => {
        console.error('ERR: Fetching list of artifacts failed', err);
      });
  }

  _fetchPlugins(params, extensions) {
    let fetchList = [];

    extensions.forEach((ext) => {
      let fetchParams = Object.assign({}, params, {
        extensionType: ext
      });
      let fetchApi = this.api.fetchPlugins(fetchParams).$promise;
      fetchList.push(fetchApi);
    });

    this.$q.all(fetchList)
      .then((res) => {
        console.info('res', res);

        this._fetchWidgets(params.namespace, res);
      }, (err) => {
        console.error('ERR: Fetching plugins', err);
      });
  }

  _fetchWidgets(namespace, pluginsList) {
    // Create request body for artifactproperties batch call
    let plugins = [];
    let availablePluginsMap = {};

    pluginsList.forEach((extension) => {
      extension.forEach((plugin) => {
        let pluginKey = `${plugin.name}-${plugin.type}`;

        let availablePluginKey = `${plugin.name}-${plugin.type}-${plugin.artifact.name}-${plugin.artifact.version}-${plugin.artifact.scope}`;

        availablePluginsMap[availablePluginKey] = {
          pluginInfo: plugin
        };

        let info = Object.assign({}, plugin.artifact, {
          properties: [
            `widgets.${pluginKey}`,
            `doc.${pluginKey}`
          ]
        });

        plugins.push(info);
      });
    });

    this.api.fetchAllPluginsProperties({ namespace }, plugins)
      .$promise
      .then((res) => {
        res.forEach((plugin) => {
          let pluginKey = Object.keys(plugin.properties)[0].split('.')[1];
          let key = `${pluginKey}-${plugin.name}-${plugin.version}-${plugin.scope}`;

          availablePluginsMap[key].doc = plugin.properties[`doc.${pluginKey}`];

          let parsedWidgets;
          let widgets = plugin.properties[`widgets.${pluginKey}`];

          if (widgets) {
            try {
              parsedWidgets = JSON.parse(widgets);
            } catch (e) {
              console.info('failed to parse widgets', e, pluginKey);
            }
          }
          availablePluginsMap[key].widgets = parsedWidgets;
        });

        console.info('availablePluginsMap', availablePluginsMap);
      });
  }
}

angular.module(`${PKG.name}.feature.hydrator`)
  .service('PipelineAvailablePluginsActions', PipelineAvailablePluginsActions);
