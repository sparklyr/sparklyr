// !preview r2d3 data=list(sources = list("FileStreamSource[file]"), sinks = list("FileSink[file]"), stats = list(list(rps = list("in" = 3, out = 2)), list(rps = list("in" = 5, out = 10)), list(rps = list("in" = 10, out = 10)), list(rps = list("in" = 10, out = 10)), list(rps = list("in" = 10, out = 10)), list(rps = list("in" = 10, out = 10)), list(rps = list("in" = 10, out = 10)), list(rps = list("in" = 10, out = 10)), list(rps = list("in" = 10, out = 10)), list(rps = list("in" = 10, out = 10)))), options = list(demo = FALSE), container = "div"

function StreamStats() {
  var rpmIn = [0];
  var rpmOut = [0];

  var maxIn = 10;
  var maxOut = 10;

  var maxStats = 100;

  this.add = function(rps) {
    if (maxIn < rps.in) maxIn = rps.in;
    if (maxOut < rps.out) maxOut = rps.out;

    rpmIn.unshift(rps.in);
    rpmOut.unshift(rps.out);

    rpmIn = rpmIn.slice(0, Math.min(rpmIn.length, maxStats - 1));
    rpmOut = rpmOut.slice(0, Math.min(rpmOut.length, maxStats - 1));
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

  this.count = function() {
    return rpmIn.length;
  };
}

var stats = new StreamStats();

function StreamRenderer(stats) {
  var self = this;
  var width = 0;
  var height = 0;

  div.attr("class", "root");
  var svg = div.append("svg");
  var chart = div.append("svg")
    .attr("class", "chart");

  chart.append("marker")
    .attr("id", "circle")
    .attr("markerWidth", "4").attr("markerHeight", "4")
    .attr("refX", "2").attr("refY", "2")
    .append("circle")
      .attr("cx", "2").attr("cy", "2").attr("r", "2")
      .attr("class", "tick");

  var ticks = chart.append("g");

  var chartIn;
  var chartOut;
  var rowsPerSecondIn;
  var rowsPerSecondOut;

  var barWidth = 10;
  var margin = 30;
  var marginLeft = margin + 10;
  var remotesCircle = 10;
  var remotesMargin = margin;
  var remotesHeight = 0;
  var chartHeight = 0;
  var barsHeight = 0;
  var annotationHeight = 20;
  var stopped = false;

  this.formatRps = function(num) {
    return d3.format(",.0f")(num);
  };

  this.setSize = function(newWidth, newHeight) {
    remotesHeight = Math.min(40, newHeight / 3);
    chartHeight = newHeight - 2 * remotesHeight - 2 * margin;
    barsHeight = chartHeight - 2 * annotationHeight;

    svg.attr("width", newWidth).attr("height", newHeight);

    chart.attr("width", newWidth).attr("height", chartHeight);
    chart.style("margin-top", margin + remotesHeight + "px");

    chart.style("display", newHeight < 120 ? "none" : "block");

    width = newWidth;
    height =  newHeight;
  };

  this.stop = function() {
    stopped = true;
  };

  this.stopped = function() {
    return stopped;
  };

  this.annotate = function(x, text) {
    ticks.append("line")
      .attr("class", "horizon")
      .attr("x1", x)
      .attr("y1", 10)
      .attr("x2", x)
      .attr("y2", chartHeight / 2)
      .attr("marker-start", "url(#circle)")
      .attr("marker-end", "url(#circle)");

    ticks.append("text")
      .attr("y", 13).attr("x", x + 4)
      .attr("class", "annotation")
      .text(text);
  };

  var animateClass = "animate";
  this.setAnimateClass = function(name) {
    animateClass = name;
  };

  this.update = function() {
    chart.attr("class", "chart");
    if (!stopped) {
      setTimeout(function() {
        chart.attr("class", "chart " + self.animateClass);
      }, 50);
    }
    else {
      chart.attr("class", "chart stop");
    }

    rowsPerSecondIn.text(self.formatRps(stats.latestRpsIn()) + " rps");
    rowsPerSecondOut.text(self.formatRps(stats.latestRpsOut()) + " rps");

    rowsPerSecondIn.style("display", height < 60 ? "none" : "block");
    rowsPerSecondOut.style("display", height < 60 ? "none" : "block");

    var dataIn = chartIn.selectAll("rect")
      .data(stats.rpmIn());

    var barsIn = dataIn
      .enter()
        .append("rect")
      .merge(dataIn)
        .attr("width", barWidth - 2)
        .attr("height", function(d, i) { return barsHeight / 2 * d / stats.maxIn();})
        .attr("x", function(d, i) { return marginLeft + i * barWidth - barWidth; })
        .attr("y", function(d, i) { return chartHeight / 2 - barsHeight / 2 * d / stats.maxIn(); })
        .attr("class", "barIn")
        .on("mouseover", function (d, i) {
          rowsPerSecondIn.text(self.formatRps(d) + " rps");
          d3.select(this.parentNode).selectAll("rect").attr("class", "barIn");
          d3.select(this).attr("class", "barIn selected");
        });

    var dataOut = chartOut.selectAll("rect")
      .data(stats.rpmOut());

    var barsOut = dataOut.
      enter()
        .append("rect")
      .merge(dataOut)
        .attr("width", barWidth - 2)
        .attr("height", function(d, i) { return barsHeight / 2 * d / stats.maxOut();})
        .attr("x", function(d, i) { return marginLeft + i * barWidth - barWidth; })
        .attr("y", function(d, i) { return chartHeight / 2; })
        .attr("class", "barOut")
        .on("mouseover", function (d, i) {
          rowsPerSecondOut.text(self.formatRps(d) + " rps");
          d3.select(this.parentNode).selectAll("rect").attr("class", "barOut");
          d3.select(this).attr("class", "barOut selected");
        });

    ticks.selectAll("*").remove();
    ticks.append("line")
      .attr("class", "horizon")
      .attr("x1", 0)
      .attr("y1", chartHeight / 2)
      .attr("x2", width - margin / 2)
      .attr("y2", chartHeight / 2);

    this.annotate(marginLeft + barWidth * (stats.count() - 1) - 4, "Start");
    if (stopped) {
      this.annotate(marginLeft - barWidth - 8, "End");
    }
  };

  this.render = function() {
    svg.selectAll("g").remove();
    svg.selectAll("text").remove();
    chart.selectAll("rect").remove();

    chartIn = chart.append("g");
    chartOut = chart.append("g");

    rpsTextSize = Math.min(10 + height / 40, 20);

    rowsPerSecondIn = svg.append("text");
    rowsPerSecondIn.style("font-size", rpsTextSize + "px");

    rowsPerSecondOut = svg.append("text");
    rowsPerSecondOut.style("font-size", rpsTextSize + "px");

    var sourceCircles = svg.selectAll(".source")
      .data(data.sources)
      .enter()
      .append("g")
        .attr("class", "source")
        .attr("transform", function(d, i) {
          return "translate(" + (marginLeft + i * 24) + ", " + remotesMargin + ")";
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

    var sourceText = svg
      .append("text")
        .attr("y", margin + 3).attr("x", marginLeft + data.sources.length * 24)
        .attr("class", "label")
        .text(function(d) { return data.sources[data.sources.length - 1]; });

    rowsPerSecondIn
      .attr("y", margin + rpsTextSize + 5).attr("x", marginLeft + data.sources.length * 24)
      .attr("class", "rps")
      .text(self.formatRps(stats.latestRpsIn()) + " rps");

    rowsPerSecondIn.append("title").text("rows per second");

    var sinkCircles = svg.selectAll(".sink")
      .data(data.sinks)
      .enter()
      .append("g")
        .attr("class", "sink")
        .attr("transform", function(d, i) {
          return "translate(" + (marginLeft + i * 24) + ","  + (height - remotesMargin) + ")";
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

    var sinkText = svg
      .append("text")
        .attr("y", height - margin + 3).attr("x", marginLeft + data.sinks.length * 24)
        .attr("class", "label sinkText")
        .text(function(d) { return data.sinks[data.sinks.length - 1]; });

    rowsPerSecondOut
      .attr("y", height - margin - 15).attr("x", marginLeft + data.sinks.length * 24)
      .attr("class", "rps")
      .text(self.formatRps(stats.latestRpsOut()) + " rps");

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

function debug(msg) {
  if (options !== null && "debug" in options) {
    console.log(msg);
  }
}

if (options !== null && "demo" in options && options.demo === true) {
  setInterval(function() {
    var data = {
      rps: {
        in: Math.random() * 100,
        out: Math.random() * 100
      }
    };

    debug(JSON.stringify(data));
    stats.add(data.rps);
    renderer.update();
  }, 1000);
}
else if (typeof(Shiny) !== "undefined") {
  Shiny.addCustomMessageHandler("sparklyr_stream_view", function(data) {
      debug(JSON.stringify(data));
      stats.add(data.rps);
      renderer.update();
    }
  );
}
else if (data !== null && "stats" in data) {
  var statsIdx = 0;
  setInterval(function(data) {
    return function() {
      if (statsIdx >= data.stats.length) {
        var stopped = renderer.stopped();
        renderer.stop();
        if (!stopped) renderer.update();
        return;
      }

      var d = data.stats[statsIdx];

      debug(JSON.stringify(d));
      stats.add(d.rps);

      renderer.setAnimateClass("animateFast");
      renderer.update();

      statsIdx += 1;
    };
  }(data), 100);
}
