/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

angular.module(PKG.name + '.commons')
  .controller('DAGPlusPlusCtrl', function MyDAGController(jsPlumb, $scope, $timeout, DAGPlusPlusFactory, GLOBALS, DAGPlusPlusNodesActionsFactory, $window, DAGPlusPlusNodesStore, $rootScope, $popover, uuid, DAGPlusPlusNodesDispatcher, HydratorPlusPlusDetailMetricsActions, NonStorePipelineErrorFactory, AvailablePluginsStore, myHelpers) {

    var vm = this;

    var dispatcher = DAGPlusPlusNodesDispatcher.getDispatcher();
    var undoListenerId = dispatcher.register('onUndoActions', resetEndpointsAndConnections);
    var redoListenerId = dispatcher.register('onRedoActions', resetEndpointsAndConnections);

    let localX, localY;

    const SHOW_METRICS_THRESHOLD = 0.8;

    const separation = $scope.separation || 200; // node separation length

    const nodeWidth = 200;
    const nodeHeight = 80;

    var dragged = false;

    vm.isDisabled = $scope.isDisabled;
    vm.disableNodeClick = $scope.disableNodeClick;

    var nodePopovers = {};
    var selectedConnections = [];
    var endpointClicked = false;
    var connectionDropped = false;
    var dagMenu;

    this.pluginsMap = {};

    vm.scale = 1.0;

    vm.panning = {
      style: {
        'top': 0,
        'left': 0
      },
      top: 0,
      left: 0
    };

    vm.comments = [];
    vm.nodeMenuOpen = null;
    vm.dagMenuOpen = null;

    var repaintTimeout,
        commentsTimeout,
        nodesTimeout,
        fitToScreenTimeout,
        initTimeout,
        nodePopoverTimeout,
        resetTimeout;

    var Mousetrap = window.CaskCommon.Mousetrap;

    function repaintEverything() {
      if (repaintTimeout) {
        $timeout.cancel(repaintTimeout);
      }

      repaintTimeout = $timeout(function () { vm.instance.repaintEverything(); });
    }

    function init() {
      $scope.nodes = DAGPlusPlusNodesStore.getNodes();
      $scope.connections = DAGPlusPlusNodesStore.getConnections();
      vm.undoStates = DAGPlusPlusNodesStore.getUndoStates();
      vm.redoStates = DAGPlusPlusNodesStore.getRedoStates();
      vm.comments = DAGPlusPlusNodesStore.getComments();

      initTimeout = $timeout(function () {
        initNodes();
        addConnections();
        vm.instance.bind('connection', addConnection);
        vm.instance.bind('connectionDetached', removeConnection);
        vm.instance.bind('connectionMoved', moveConnection);
        vm.instance.bind('beforeStartDetach', onStartDetach);
        vm.instance.bind('beforeDrop', checkIfConnectionExistsOrValid);
        vm.instance.bind('beforeDrag', onBeforeDrag);
        // jsPlumb docs say the event for clicking on an endpoint is called 'endpointClick',
        // but seems like the 'click' event is triggered both when clicking on an endpoint &&
        // clicking on a connection
        vm.instance.bind('click', toggleConnections);

        Mousetrap.bind(['command+z', 'ctrl+z'], vm.undoActions);
        Mousetrap.bind(['command+shift+z', 'ctrl+shift+z'], vm.redoActions);
        Mousetrap.bind(['del', 'backspace'], vm.removeSelectedConnections);

        if (vm.isDisabled) {
          // Disable all endpoints
          angular.forEach($scope.nodes, function (node) {
            var endpointArr = vm.instance.getEndpoints(node.name);

            if (endpointArr) {
              angular.forEach(endpointArr, function (endpoint) {
                endpoint.setEnabled(false);
              });
            }
          });
        }

        // Process metrics data
        if ($scope.showMetrics) {

          angular.forEach($scope.nodes, function (node) {
            var elem = angular.element(document.getElementById(node.name)).children();

            var scope = $rootScope.$new();
            scope.data = {
              node: node
            };
            scope.version = node.plugin.artifact.version;

            nodePopovers[node.name] = {
              scope: scope,
              element: elem,
              popover: null,
              isShowing: false
            };

            $scope.$on('$destroy', function () {
              elem.remove();
              elem = null;
              scope.$destroy();
            });

          });

          $scope.$watch('metricsData', function () {
            if (Object.keys($scope.metricsData).length === 0) {
              angular.forEach(nodePopovers, function (value) {
                value.scope.data.metrics = 0;
              });
            }

            angular.forEach($scope.metricsData, function (value, key) {
              nodePopovers[key].scope.data.metrics = value;
            });
          }, true);
        }
      });

      // This is here because the left panel is initially in the minimized mode and expands
      // based on user setting on local storage. This is taking more than a single angular digest cycle
      // Hence the timeout to 1sec to render it in subsequent digest cycles.
      // FIXME: This directive should not be dependent on specific external component to render itself.
      // The left panel should default to expanded view and cleaning up the graph and fit to screen should happen in parallel.
      fitToScreenTimeout = $timeout(() => {
        vm.cleanUpGraph();
        vm.fitToScreen();
      }, 500);
    }

    function closeNodePopover(node) {
      var nodeInfo = nodePopovers[node.name];
      if (nodePopoverTimeout) {
        $timeout.cancel(nodePopoverTimeout);
      }
      if (nodeInfo && nodeInfo.popover) {
        nodeInfo.popover.hide();
        nodeInfo.popover.destroy();
        nodeInfo.popover = null;
      }
    }

    vm.nodeMouseEnter = function (node) {
      if (!$scope.showMetrics || vm.scale >= SHOW_METRICS_THRESHOLD) { return; }

      var nodeInfo = nodePopovers[node.name];

      if (nodePopoverTimeout) {
        $timeout.cancel(nodePopoverTimeout);
      }

      if (nodeInfo.element && nodeInfo.scope) {
        nodeInfo.popover = $popover(nodeInfo.element, {
          trigger: 'manual',
          placement: 'auto right',
          target: angular.element(nodeInfo.element[0]),
          templateUrl: $scope.nodePopoverTemplate,
          container: 'main',
          scope: nodeInfo.scope
        });
        nodeInfo.popover.$promise
          .then(function () {

            // Needs a timeout here to avoid showing popups instantly when just moving
            // cursor across a node
            nodePopoverTimeout = $timeout(function () {
              if (nodeInfo.popover && typeof nodeInfo.popover.show === 'function') {
                nodeInfo.popover.show();
              }
            }, 500);
          });
      }
    };

    vm.nodeMouseLeave = function (node) {
      if (!$scope.showMetrics || vm.scale >= SHOW_METRICS_THRESHOLD) { return; }

      closeNodePopover(node);
    };

    vm.zoomIn = function () {
      vm.scale += 0.1;

      setZoom(vm.scale, vm.instance);
    };

    vm.zoomOut = function () {
      if (vm.scale <= 0.2) { return; }

      vm.scale -= 0.1;
      setZoom(vm.scale, vm.instance);
    };

    /**
     * Utily function from jsPlumb
     * https://jsplumbtoolkit.com/community/doc/zooming.html
     *
     * slightly modified to fit our needs
     **/
    function setZoom(zoom, instance, transformOrigin, el) {
      if ($scope.nodes.length === 0) { return; }

      transformOrigin = transformOrigin || [ 0.5, 0.5 ];
      instance = instance || jsPlumb;
      el = el || instance.getContainer();
      var p = [ 'webkit', 'moz', 'ms', 'o' ],
          s = 'scale(' + zoom + ')',
          oString = (transformOrigin[0] * 100) + '% ' + (transformOrigin[1] * 100) + '%';

      for (var i = 0; i < p.length; i++) {
        el.style[p[i] + 'Transform'] = s;
        el.style[p[i] + 'TransformOrigin'] = oString;
      }

      el.style['transform'] = s;
      el.style['transformOrigin'] = oString;

      instance.setZoom(zoom);
    }

    function initNodes() {
      angular.forEach($scope.nodes, function (node) {
        let sourceObj = {
          isSource: true,
          filter: function(event) {
            // we need this variable because when the user clicks on the endpoint circle, multiple
            // 'mousedown' events are fired
            if (event.target.className === 'endpoint-circle' && !endpointClicked) {
              endpointClicked = true;
            }

            return event.target.className === 'endpoint-circle';
          },
          connectionType: 'basic'
        };
        if (vm.isDisabled) {
          sourceObj.enabled = false;
        }
        vm.instance.makeSource(node.name, sourceObj);

        vm.instance.makeTarget(node.name, {
          isTarget: true,
          dropOptions: { hoverClass: 'drag-hover' },
          anchor: 'ContinuousLeft',
          allowLoopback: false
        });
      });
    }

    function addConnections() {
      angular.forEach($scope.connections, function (conn) {
        var sourceNode = $scope.nodes.find( node => node.name === conn.from);
        var targetNode = $scope.nodes.find( node => node.name === conn.to);

        if (!sourceNode || !targetNode) {
          return;
        }

        var connObj = {
          source: conn.from,
          target: conn.to
        };

        vm.instance.connect(connObj);
      });
    }

    function transformCanvas (top, left) {
      vm.panning.top += top;
      vm.panning.left += left;

      vm.panning.style = {
        'top': vm.panning.top + 'px',
        'left': vm.panning.left + 'px'
      };
    }

    function addConnection(newConnObj) {
      $scope.connections.push({
        from: newConnObj.sourceId,
        to: newConnObj.targetId
      });
      DAGPlusPlusNodesActionsFactory.setConnections($scope.connections);
    }

    function removeConnection(detachedConnObj, updateStore = true) {
      var connectionIndex = _.findIndex($scope.connections, function (conn) {
        return conn.from === detachedConnObj.sourceId && conn.to === detachedConnObj.targetId;
      });
      if (connectionIndex !== -1) {
        $scope.connections.splice(connectionIndex, 1);
      }
      if (updateStore) {
        DAGPlusPlusNodesActionsFactory.setConnections($scope.connections);
      }
    }

    function moveConnection(moveInfo) {
      let oldConnection = {
        sourceId: moveInfo.originalSourceId,
        targetId: moveInfo.originalTargetId
      };
      // don't need to call addConnection for the new connection, since that will be done
      // automatically as part of the 'connection' event
      removeConnection(oldConnection, false);
    }

    vm.removeSelectedConnections = function() {
      if (selectedConnections.length === 0 || vm.isDisabled) { return; }

      vm.instance.unbind('connectionDetached');
      angular.forEach(selectedConnections, function (selectedConnectionObj) {
        removeContextMenuEventListener(selectedConnectionObj);
        vm.instance.detach(selectedConnectionObj);
        removeConnection(selectedConnectionObj, false);
      });
      vm.instance.bind('connectionDetached', removeConnection);
      selectedConnections = [];
      DAGPlusPlusNodesActionsFactory.setConnections($scope.connections);
    };

    function toggleConnections(seletectedObj) {
      // is endpoint
      if (seletectedObj.endpoint) {
        if (seletectedObj.connections && seletectedObj.connections.length > 0) {
          let connectionsToToggle = vm.instance.getConnections({
            source: seletectedObj.connections[0].sourceId
          });

          let notYetSelectedConnections = _.difference(connectionsToToggle, selectedConnections);

          // This is to toggle all connections coming from an endpoint.
          // If zero, one or more (but not all) of the connections are already selected,
          // then just select the remaining ones. Else if they're all selected,
          // then unselect them.

          if (notYetSelectedConnections.length !== 0) {
            notYetSelectedConnections.forEach(connection => {
              selectedConnections.push(connection);
              connection.getConnector().canvas.addEventListener('contextmenu', openContextMenu);
              connection.setType('selected');
            });
          } else {
            connectionsToToggle.forEach(connection => {
              selectedConnections.splice(selectedConnections.indexOf(connection), 1);
              removeContextMenuEventListener(connection);
              connection.setType('basic');
            });
          }
        }

      // is connection
      } else {
        if (vm.isDisabled) { return; }

        if (selectedConnections.indexOf(seletectedObj) === -1) {
          selectedConnections.push(seletectedObj);
          seletectedObj.getConnector().canvas.addEventListener('contextmenu', openContextMenu);
        } else {
          selectedConnections.splice(selectedConnections.indexOf(seletectedObj), 1);
          removeContextMenuEventListener(seletectedObj);
        }
        seletectedObj.toggleType('selected');

      }
    }

    function onStartDetach() {
      connectionDropped = false;
    }

    function checkIfConnectionExistsOrValid(connObj) {
      // return false if connection already exists, which will prevent the connecton from being formed
      if (connectionDropped) { return false; }

      var exists = _.find($scope.connections, function (conn) {
        return conn.from === connObj.sourceId && conn.to === connObj.targetId;
      });

      var sameNode = connObj.sourceId === connObj.targetId;

      if (exists || sameNode) {
        connectionDropped = true;
        return false;
      }

      // else check if the connection is valid
      var fromNode = connObj.sourceId,
          toNode = connObj.targetId;

      angular.forEach($scope.nodes, function (node) {
        if (node.name === fromNode) {
          fromNode = node;
        } else if (node.name === toNode) {
          toNode = node;
        }
      });

      var valid = true;

      NonStorePipelineErrorFactory.connectionIsValid(fromNode, toNode, function(invalidConnection) {
        if (invalidConnection) { valid = false; }
      });
      connectionDropped = true;
      return valid;
    }

    function onBeforeDrag() {
      if (endpointClicked) {
        endpointClicked = false;
        connectionDropped = false;
      }
    }

    function resetEndpointsAndConnections() {
      if (resetTimeout) {
        $timeout.cancel(resetTimeout);
      }

      resetTimeout = $timeout(function () {
        vm.instance.reset();

        $scope.nodes = DAGPlusPlusNodesStore.getNodes();
        $scope.connections = DAGPlusPlusNodesStore.getConnections();
        vm.undoStates = DAGPlusPlusNodesStore.getUndoStates();
        vm.redoStates = DAGPlusPlusNodesStore.getRedoStates();
        initNodes();
        addConnections();
        angular.forEach(selectedConnections, function(selectedConnObj) {
          removeContextMenuEventListener(selectedConnObj);
        });
        selectedConnections = [];
        makeNodesDraggable();
        vm.instance.bind('connection', addConnection);
        vm.instance.bind('connectionMoved', moveConnection);
        vm.instance.bind('connectionDetached', removeConnection);
        vm.instance.bind('beforeDrop', checkIfConnectionExistsOrValid);
        vm.instance.bind('beforeStartDetach', onStartDetach);
        vm.instance.bind('beforeDrag', onBeforeDrag);
        vm.instance.bind('click', toggleConnections);

        if (commentsTimeout) {
          vm.comments = DAGPlusPlusNodesStore.getComments();
          $timeout.cancel(commentsTimeout);
        }

        commentsTimeout = $timeout(function () {
          makeCommentsDraggable();
        });
      });
    }

    function makeNodesDraggable() {
      var nodes = document.querySelectorAll('.box');

      if (!vm.isDisabled) {
        vm.instance.draggable(nodes, {
          start: function (drag) {
            let currentCoOrdinates = {
              x: drag.e.clientX,
              y: drag.e.clientY,
            };
            if (currentCoOrdinates.x === localX && currentCoOrdinates.y === localY) {
              return;
            }
            localX = currentCoOrdinates.x;
            localY = currentCoOrdinates.y;

            dragged = true;
          },
          stop: function (dragEndEvent) {
            var config = {
              _uiPosition: {
                top: dragEndEvent.el.style.top,
                left: dragEndEvent.el.style.left
              }
            };
            DAGPlusPlusNodesActionsFactory.updateNode(dragEndEvent.el.id, config);
          }
        });
      }
    }

    function makeCommentsDraggable() {
      var comments = document.querySelectorAll('.comment-box');
      vm.instance.draggable(comments, {
        start: function () {
          dragged = true;
        },
        stop: function (dragEndEvent) {
          var config = {
            _uiPosition: {
              top: dragEndEvent.el.style.top,
              left: dragEndEvent.el.style.left
            }
          };
          DAGPlusPlusNodesActionsFactory.updateComment(dragEndEvent.el.id, config);
        }
      });
    }

    function getPosition(e) {
      var posx = 0;
      var posy = 0;

      if (e.pageX || e.pageY) {
        posx = e.pageX;
        posy = e.pageY;
      } else if (e.clientX || e.clientY) {
        posx = e.clientX + document.body.scrollLeft + document.documentElement.scrollLeft;
        posy = e.clientY + document.body.scrollTop + document.documentElement.scrollTop;
      }

      return {
        x: posx,
        y: posy
      };
    }

    function positionContextMenu(e, menu) {
      var menuPosition = getPosition(e);
      menu.style.left = menuPosition.x + 'px';
      menu.style.top = menuPosition.y + 'px';
    }

    vm.selectEndpoint = function(event, node) {
      if (event.target.className === 'endpoint-circle') {
        let sourceElem = node.name;
        let endpoints = vm.instance.getEndpoints(sourceElem);
        if (endpoints) {
          for (let i = 0; i < endpoints.length; i++) {
            let endpoint = endpoints[i];
            if (endpoint.connections && endpoint.connections.length > 0) {
              if (endpoint.connections[0].sourceId === node.name) {
                toggleConnections(endpoint);
                break;
              }
            }
          }
        }
      }
      endpointClicked = false;
    };

    function openContextMenu(e) {
      if (selectedConnections.length > 0) {
        e.preventDefault();
        vm.openDagMenu(true);
        positionContextMenu(e, dagMenu);
      }
    }

    function removeContextMenuEventListener(connection) {
      connection.getConnector().canvas.removeEventListener('contextmenu', openContextMenu);
    }

    jsPlumb.ready(function() {
      var dagSettings = DAGPlusPlusFactory.getSettings().default;
      var dagSelectedConnectionStyle = DAGPlusPlusFactory.getSettings().selectedConnectionStyle;

      jsPlumb.setContainer('dag-container');
      vm.instance = jsPlumb.getInstance(dagSettings);
      vm.instance.registerConnectionType('selected', dagSelectedConnectionStyle);

      init();

      dagMenu = document.querySelector('.dag-popover-menu');

      // Making canvas draggable
      vm.secondInstance = jsPlumb.getInstance();
      if (!vm.disableNodeClick) {
        vm.secondInstance.draggable('diagram-container', {
          stop: function (e) {
            e.el.style.left = '0px';
            e.el.style.top = '0px';
            transformCanvas(e.pos[1], e.pos[0]);
            DAGPlusPlusNodesActionsFactory.resetPluginCount();
            DAGPlusPlusNodesActionsFactory.setCanvasPanning(vm.panning);
          }
        });
      }

      // doing this to listen to changes to just $scope.nodes instead of everything else
      $scope.$watch('nodes', function() {
        if (!vm.isDisabled) {
          if (nodesTimeout) {
            $timeout.cancel(nodesTimeout);
          }
          nodesTimeout = $timeout(function () {
            initNodes();
            makeNodesDraggable();
          });
        }
      }, true);

      // This is needed to redraw connections and endpoints on browser resize
      angular.element($window).on('resize', vm.instance.repaintEverything);

      DAGPlusPlusNodesStore.registerOnChangeListener(function () {
        vm.activeNodeId = DAGPlusPlusNodesStore.getActiveNodeId();

        // can do keybindings only if no node is selected
        if (!vm.activeNodeId) {
          Mousetrap.bind(['command+z', 'ctrl+z'], vm.undoActions);
          Mousetrap.bind(['command+shift+z', 'ctrl+shift+z'], vm.redoActions);
        } else {
          Mousetrap.unbind(['command+z', 'ctrl+z']);
          Mousetrap.unbind(['command+shift+z', 'ctrl+shift+z']);
        }
      });

    });

    vm.onNodeClick = function(event, node) {
      event.stopPropagation();
      closeNodePopover(node);
      HydratorPlusPlusDetailMetricsActions.setMetricsTabActive(false);
      DAGPlusPlusNodesActionsFactory.selectNode(node.name);
    };

    vm.onMetricsClick = function(event, node) {
      event.stopPropagation();
      closeNodePopover(node);
      HydratorPlusPlusDetailMetricsActions.setMetricsTabActive(true);
      DAGPlusPlusNodesActionsFactory.selectNode(node.name);
    };

    vm.onNodeDelete = function (event, node) {
      event.stopPropagation();
      DAGPlusPlusNodesActionsFactory.removeNode(node.name);
      vm.instance.unbind('connectionDetached');
      selectedConnections = selectedConnections.filter(function(selectedConnObj) {
        if (selectedConnObj.sourceId === node.name || selectedConnObj.targetId === node.name) {
          removeContextMenuEventListener(selectedConnObj);
        }
        return selectedConnObj.sourceId !== node.name && selectedConnObj.targetId !== node.name;
      });
      vm.instance.remove(node.name);
      vm.instance.bind('connectionDetached', removeConnection);
    };

    vm.cleanUpGraph = function () {
      if ($scope.nodes.length === 0) { return; }

      var graphNodes = DAGPlusPlusFactory.getGraphLayout($scope.nodes, $scope.connections, separation)._nodes;
      angular.forEach($scope.nodes, function (node) {
        var location = graphNodes[node.name];
        node._uiPosition = {
          left: location.x - 50 + 'px',
          top: location.y + 'px'
        };
      });

      $scope.getGraphMargins($scope.nodes);

      vm.panning.top = 0;
      vm.panning.left = 0;

      vm.panning.style = {
        'top': vm.panning.top + 'px',
        'left': vm.panning.left + 'px'
      };

      repaintEverything();

      DAGPlusPlusNodesActionsFactory.resetPluginCount();
      DAGPlusPlusNodesActionsFactory.setCanvasPanning(vm.panning);
    };

    vm.toggleNodeMenu = function (nodeName, event) {
      if (event) {
        event.preventDefault();
        event.stopPropagation();
      }

      if (vm.nodeMenuOpen === nodeName) {
        vm.nodeMenuOpen = null;
      } else {
        vm.nodeMenuOpen = nodeName;
      }
    };


    vm.openDagMenu = function(open) {
      vm.dagMenuOpen = open;
    };

    // This algorithm is f* up
    vm.fitToScreen = function () {
      if ($scope.nodes.length === 0) { return; }

      /**
       * Need to find the furthest nodes:
       * 1. Left most nodes
       * 2. Right most nodes
       * 3. Top most nodes
       * 4. Bottom most nodes
       **/
      var minLeft = _.min($scope.nodes, function (node) {
        if (node._uiPosition.left.indexOf('vw') !== -1) {
          var left = parseInt(node._uiPosition.left, 10)/100 * document.documentElement.clientWidth;
          node._uiPosition.left = left + 'px';
        }
        return parseInt(node._uiPosition.left, 10);
      });
      var maxLeft = _.max($scope.nodes, function (node) {
        if (node._uiPosition.left.indexOf('vw') !== -1) {
          var left = parseInt(node._uiPosition.left, 10)/100 * document.documentElement.clientWidth;
          node._uiPosition.left = left + 'px';
        }
        return parseInt(node._uiPosition.left, 10);
      });

      var minTop = _.min($scope.nodes, function (node) {
        return parseInt(node._uiPosition.top, 10);
      });

      var maxTop = _.max($scope.nodes, function (node) {
        return parseInt(node._uiPosition.top, 10);
      });

      /**
       * Calculate the max width and height of the actual diagram by calculating the difference
       * between the furthest nodes
       **/
      var width = parseInt(maxLeft._uiPosition.left, 10) - parseInt(minLeft._uiPosition.left, 10) + nodeWidth;
      var height = parseInt(maxTop._uiPosition.top, 10) - parseInt(minTop._uiPosition.top, 10) + nodeHeight;

      var parent = $scope.element[0].parentElement.getBoundingClientRect();

      // margins from the furthest nodes to the edge of the canvas (75px each)
      var leftRightMargins = 150;
      var topBottomMargins = 150;

      // calculating the scales and finding the minimum scale
      var widthScale = (parent.width - leftRightMargins) / width;
      var heightScale = (parent.height - topBottomMargins) / height;

      vm.scale = Math.min(widthScale, heightScale);

      if (vm.scale > 1) {
        vm.scale = 1;
      }
      setZoom(vm.scale, vm.instance);


      // This will move all nodes by the minimum left and minimum top
      var offsetLeft = parseInt(minLeft._uiPosition.left, 10);
      angular.forEach($scope.nodes, function (node) {
        node._uiPosition.left = (parseInt(node._uiPosition.left, 10) - offsetLeft) + 'px';
      });

      var offsetTop = parseInt(minTop._uiPosition.top, 10);
      angular.forEach($scope.nodes, function (node) {
        node._uiPosition.top = (parseInt(node._uiPosition.top, 10) - offsetTop) + 'px';
      });

      $scope.getGraphMargins($scope.nodes);

      repaintEverything();

      vm.panning.left = 0;
      vm.panning.top = 0;

      vm.panning.style = {
        'top': vm.panning.top + 'px',
        'left': vm.panning.left + 'px'
      };

      DAGPlusPlusNodesActionsFactory.resetPluginCount();
      DAGPlusPlusNodesActionsFactory.setCanvasPanning(vm.panning);
    };

    vm.addComment = function () {
      var canvasPanning = DAGPlusPlusNodesStore.getCanvasPanning();

      var config = {
        content: '',
        isActive: false,
        id: 'comment-' + uuid.v4(),
        _uiPosition: {
          'top': 250 - canvasPanning.top + 'px',
          'left': (10/100 * document.documentElement.clientWidth) - canvasPanning.left + 'px'
        }
      };

      DAGPlusPlusNodesActionsFactory.addComment(config);
    };

    function clearCommentSelection() {
      angular.forEach(vm.comments, function (comment) {
        comment.isActive = false;
      });
    }

    vm.commentSelect = function (event, comment) {
      event.stopPropagation();
      clearCommentSelection();

      if (dragged) {
        dragged = false;
        return;
      }

      comment.isActive = true;
    };

    vm.deleteComment = function (comment) {
      DAGPlusPlusNodesActionsFactory.deleteComment(comment);
    };

    vm.undoActions = function () {
      if (!vm.isDisabled && vm.undoStates.length > 0) {
        DAGPlusPlusNodesActionsFactory.undoActions();
      }
    };

    vm.redoActions = function () {
      if (!vm.isDisabled && vm.redoStates.length > 0) {
        DAGPlusPlusNodesActionsFactory.redoActions();
      }
    };


    // CUSTOM ICONS CONTROL
    function generatePluginMapKey(node) {
      let plugin = node.plugin;

      return `${plugin.name}-${node.type}-${plugin.artifact.name}-${plugin.artifact.version}-${plugin.artifact.scope}`;
    }

    vm.shouldShowCustomIcon = (node) => {
      let key = generatePluginMapKey(node);

      let iconSourceType = myHelpers.objectQuery(this.pluginsMap, key, 'widgets', 'icon', 'type');

      return ['inline', 'link'].indexOf(iconSourceType) !== -1;
    };

    vm.getCustomIconSrc = (node) => {
      let key = generatePluginMapKey(node);
      let iconSourceType = myHelpers.objectQuery(this.pluginsMap, key, 'widgets', 'icon', 'type');

      if (iconSourceType === 'inline') {
        return myHelpers.objectQuery(this.pluginsMap, key, 'widgets', 'icon', 'arguments', 'data');
      }

      return myHelpers.objectQuery(this.pluginsMap, key, 'widgets', 'icon', 'arguments', 'url');
    };


    let subAvailablePlugins = AvailablePluginsStore.subscribe(() => {
      this.pluginsMap = AvailablePluginsStore.getState().plugins.pluginsMap;
    });

    $scope.$on('$destroy', function () {
      DAGPlusPlusNodesActionsFactory.resetNodesAndConnections();
      DAGPlusPlusNodesStore.reset();

      subAvailablePlugins();

      angular.element($window).off('resize', vm.instance.repaintEverything);

      // Cancelling all timeouts, key bindings and event listeners
      $timeout.cancel(repaintTimeout);
      $timeout.cancel(commentsTimeout);
      $timeout.cancel(nodesTimeout);
      $timeout.cancel(fitToScreenTimeout);
      $timeout.cancel(initTimeout);
      $timeout.cancel(nodePopoverTimeout);
      angular.forEach(selectedConnections, function(selectedConnObj) {
        removeContextMenuEventListener(selectedConnObj);
      });
      Mousetrap.reset();
      dispatcher.unregister('onUndoActions', undoListenerId);
      dispatcher.unregister('onRedoActions', redoListenerId);
      vm.instance.reset();
    });
  });
