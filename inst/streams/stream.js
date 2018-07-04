// !preview r2d3 data=list(sources = list("FileStreamSource[file]", "Other[file]"), sinks = list("FileSink[file]")), options = list(demo = TRUE)

function StreamStats() {
  var rpmIn = [0];
  var rpmOut = [0];

  var maxIn = 100;
  var maxOut = 100;

  this.add = function(rps) {
    if (maxIn < rps.in) maxIn = rps.in;
    if (maxOut < rps.out) maxOut = rps.out;

    rpmIn.unshift(rps.in);
    rpmOut.unshift(rps.out);
  };

  this.rpmIn = function() {
    return rpmIn;
  };

  this.rpmOut = function() {
    return rpmOut;
  };

  this.maxIn = function() {
    return maxIn;
  };

  this.maxOut = function() {
    return maxOut;
  };

  this.latestRpsIn = function() {
    return rpmIn[0];
  };

  this.latestRpsOut = function() {
    return rpmOut[0];
  };
}

var stats = new StreamStats();

function StreamRenderer(stats) {
  var width = 0;
  var height = 0;

  var root;
  var chartIn;
  var chartOut;
  var rowsPerSecondIn;
  var rowsPerSecondOut;

  var barWidth = 10;
  var margin = 20;
  var remotesCircle = 10;
  var remotesMargin = margin + remotesCircle;
  var remotesHeight = 80;

  this.setSize = function(newWidth, newHeight) {
    width = newWidth;
    height =  newHeight;
  };

  this.update = function() {
    rowsPerSecondIn.text(stats.latestRpsIn() + " rps");
    rowsPerSecondOut.text(stats.latestRpsOut() + " rps");

    var chartHeight = height - 2 * remotesHeight;

    var dataIn = chartIn.selectAll("rect")
      .data(stats.rpmIn());

    var barsIn = dataIn
      .enter()
        .append("rect")
      .merge(dataIn)
        .attr("width", barWidth - 2)
        .attr("height", function(d, i) { return chartHeight / 2 * d / stats.maxIn();})
        .attr("x", function(d, i) { return margin + i * barWidth - barWidth; })
        .attr("y", function(d, i) { return remotesHeight + chartHeight / 2 - chartHeight / 2 * d / stats.maxIn(); })
        .attr("class", "barIn");

    barsIn
      .transition()
        .ease(d3.easeLinear)
        .duration(900)
        .attr("x", function(d, i) { return margin + i * barWidth; });

    var dataOut = chartOut.selectAll("rect")
      .data(stats.rpmOut());

    var barsOut = dataOut.
      enter()
        .append("rect")
      .merge(dataOut)
        .attr("width", barWidth - 2)
        .attr("height", function(d, i) { return chartHeight / 2 * d / stats.maxOut();})
        .attr("x", function(d, i) { return margin + i * barWidth - barWidth; })
        .attr("y", function(d, i) { return remotesHeight + chartHeight / 2; })
        .attr("class", "barOut");

    barsOut
      .transition()
        .ease(d3.easeLinear)
        .duration(900)
        .attr("x", function(d, i) { return margin + i * barWidth; });
  };

  this.render = function() {
    svg.selectAll("g").remove();
    root = svg.append("g");

    chartIn = root.append("g");
    chartOut = root.append("g");

    rowsPerSecondIn = root.append("text");
    rowsPerSecondOut = root.append("text");

    var sourceCircles = root.selectAll(".source")
      .data(data.sources)
      .enter()
      .append("g")
        .attr("class", "source")
        .attr("transform", function(d, i) {
          return "translate(" + (remotesMargin + i * 24) + ", " + remotesMargin + ")";
        })
        .append("circle");

    sourceCircles.attr("cx", "0").attr("cy", "0").attr("r", "10")
      .attr("class", function (d, i) {
        return (i == data.sources.length - 1) ? "source selected" : "source";
      })
      .attr("stroke-width", "2")
      .on("mouseover", function (d, i) {
        sourceCircles.attr("class", "source");
        d3.select(this).attr("class", "source selected");
        sourceText.text(d);
      });

    var sourceText = root
      .append("text")
        .attr("y", 35).attr("x", 24 + data.sources.length * 24)
        .attr("class", "label")
        .text(function(d) { return data.sources[data.sources.length - 1]; });

    rowsPerSecondIn
      .attr("y", 60).attr("x", 24 + data.sources.length * 24)
      .attr("class", "rps")
      .text(stats.latestRpsIn() + " rps");

    rowsPerSecondIn.append("title").text("rows per second");

    var sinkCircles = root.selectAll(".sink")
      .data(data.sinks)
      .enter()
      .append("g")
        .attr("class", "sink")
        .attr("transform", function(d, i) {
          return "translate(" + (remotesMargin + i * 24) + ","  + (height - remotesMargin) + ")";
        })
        .append("circle");

    sinkCircles.attr("cx", "0").attr("cy", "0").attr("r", "10")
      .attr("class", function (d, i) {
        return (i == data.sinks.length - 1) ? "sink selected" : "sink";
      })
      .attr("stroke-width", "2")
      .on("mouseover", function (d, i) {
        sinkCircles.attr("class", "sink");
        d3.select(this).attr("class", "sink selected");
        sinkText.text(d);
      });

    var sinkText = root
      .append("text")
        .attr("y", height - 25).attr("x", 24 + data.sinks.length * 24)
        .attr("class", "label sinkText")
        .text(function(d) { return data.sources[data.sinks.length - 1]; });

    rowsPerSecondOut
      .attr("y", height - 50).attr("x", 24 + data.sinks.length * 24)
      .attr("class", "rps")
      .text(stats.latestRpsOut() + " rps");

    rowsPerSecondOut.append("title").text("rows per second");

    this.update();
  };
}

var renderer = new StreamRenderer(stats);

r2d3.onRender(function(data, svg, width, height, options) {
  renderer.setSize(width, height);
  renderer.render();
});

r2d3.onResize(function(width, height) {
  renderer.setSize(width, height);
  renderer.render();
});

if (options.demo) {
  setInterval(function() {
    var data = {
      rps: {
        in: Math.floor(Math.random() * 100),
        out: Math.floor(Math.random() * 100)
      }
    };

    stats.add(data.rps);

    renderer.update();
  }, 1000);
}
