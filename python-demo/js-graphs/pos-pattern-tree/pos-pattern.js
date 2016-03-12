$(document).ready(function() {
    readFile();
});

/*
  This function will help to edges lists using
  the nodes and the connection link.
  */
var createNodeEdgeDataSet = function(edgeList, nodesList){
  //Edges for vis.js
  edgesDataVis = [];
  for(var key in edgeList){
    var wFreq = edgeList[key];
    wFreq.nGram.forEach(function(element, indexInner){
      //Skip if its single node
      if(wFreq.nGram.length > indexInner+1){
        if(wFreq.nGram.length > 1){
          edgesDataVis.push({
            from: nodesList.indexOf(element),
            to: nodesList.indexOf(wFreq.nGram[indexInner+1])
          });
        }
      }
    });
  }
  return edgesDataVis;
}

/*
  This function will allows to create the node dataset
  for vis.js
  */
var createNodeDataSet = function(nodesList){
  nodesDataVis = [];
  nodesList[1].forEach(function(element, index){
    var customLabel = "";
    if(element in nodesList[0]) { //Customize the label with frequency
      customLabel = element + ":" + nodesList[0][element].frequency;
    }
    else {
      customLabel = element;
    }
    nodesDataVis.push({id: index, label: customLabel});
  });
  return nodesDataVis;
}

/*
  To create n-grams from 0-length. This n-grams helps
  to generate the network between each nodes
  */
var nGram = function(data) {
  tokens = data.split(" ")
  posPattern = []
  for (var i = 0; i < tokens.length; i++) {
    posPattern.push(tokens.slice(0,i+1).join(' '));
  }
  return posPattern;
}

/*
  This fnction will allows the raw data to convert
  in to structed data collection.
  This will return two values:
    1) WordFrequency object list (networkConnectionList)
    2) Unique node list
  */
var createNetWorkData = function(data){
  //Get unique list
  Array.prototype.getUnique = function(){
   var u = {}, a = [];
   for(var i = 0, l = this.length; i < l; ++i){
      if(u.hasOwnProperty(this[i])) {
         continue;
      }
      a.push(this[i]);
      u[this[i]] = 1;
   }
   return a;
 }

  //Varibles that holds the last return values
  networkConnectionList = {};
  nodesList = [];

  dataList = data.split(/\n/);
  for(var i=0; i < dataList.length; i++){
    patternFrequency = dataList[i].split(/\t/);
    patternFrequency[0] = patternFrequency[0].replace(" .", "");
    var nodes = nGram(patternFrequency[0]);
    nodesList = nodesList.concat(nodes);
    networkConnectionList[patternFrequency[0]] = new WordFrequency(
      patternFrequency[0],
      patternFrequency[1],nodes);
  }

  return [networkConnectionList, nodesList.getUnique()];
}

//Read the local file
var readFile = function(){
  var fileInput = document.getElementById('fileInput');
  var fileDisplayArea = document.getElementById('fileDisplayArea');

  fileInput.addEventListener('change', function(e) {
  	var file = fileInput.files[0];
  	var textType = /text.*/;

  	if (file.type.match(textType)) {
  		var reader = new FileReader();

      //In file load event process the data and draw graph
  		reader.onload = function(e) {
  			//fileDisplayArea.innerText = reader.result;
        var networkList = createNetWorkData(reader.result);
        var nodes = createNodeDataSet(networkList);
        var edges = createNodeEdgeDataSet(networkList[0], networkList[1]);
        drawGraph(nodes, edges)
  		}

  		reader.readAsText(file);
  	} else {
  		fileDisplayArea.innerText = "File not supported!"
  	}
  });
}

//Vis.js draw function
var drawGraph = function(nodesDataVis, edgesDataVis){
  // create a network
  var container = document.getElementById('mynetwork');
  var data = {
    nodes: nodesDataVis,
    edges: edgesDataVis
  };
  var options = {
    edges: {
      smooth: true,
      arrows: {to : true }
    }
  };
  var network = new vis.Network(container, data, options);
}

//WordFrequency class
function WordFrequency(pattern, frq, ngram) {
  this.exactPattern = pattern;
  this.frequency = frq;
  this.nGram = ngram;
}
