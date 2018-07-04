// !preview r2d3 data=list(sources = list("FileStreamSource[file]", "Other[file]"), sinks = list("FileSink[file]", "x"), demo = TRUE)

var barHeight = Math.ceil(height / data.length);

var sourceCircles = svg.selectAll(".source")
  .data(data.sources)
  .enter()
  .append("g")
    .attr("class", "source")
    .attr("transform", function(d, i) {
      return "translate(" + (30 + i * 24) + ", 30)";
    })
    .append("circle");

sourceCircles.attr("cx", "0").attr("cy", "0").attr("r", "10")
  .attr("stroke", function (d, i) {
    return (i == data.sources.length - 1) ? "orange" : "white";
  })
  .attr("stroke-width", "2")
  .attr("fill", "orange")
  .on("mouseover", function (d, i) {
    sourceCircles.attr("stroke", "white");
    d3.select(this).attr("stroke", "orange");
    sourceText.text(d);
  });

var sourceText = svg
  .append("text")
    .attr("y", 35).attr("x", 24 + data.sources.length * 24)
    .attr("class", "label")
    .text(function(d) { return data.sources[data.sources.length - 1]; });

var rowsPerSecondIn = svg
  .append("text")
    .attr("y", 60).attr("x", 24 + data.sources.length * 24)
    .attr("class", "rps")
    .text("0 rps");

rowsPerSecondIn.append("title").text("rows per second");

var sinkCircles = svg.selectAll(".sink")
  .data(data.sinks)
  .enter()
  .append("g")
    .attr("class", "sink")
    .attr("transform", function(d, i) {
      return "translate(" + (30 + i * 24) + ","  + (height - 30) + ")";
    })
    .append("circle");

sinkCircles.attr("cx", "0").attr("cy", "0").attr("r", "10")
  .attr("stroke", function (d, i) {
    return (i == data.sinks.length - 1) ? "steelblue" : "white";
  })
  .attr("stroke-width", "2")
  .attr("fill", "steelblue")
  .on("mouseover", function (d, i) {
    sinkCircles.attr("stroke", "white");
    d3.select(this).attr("stroke", "steelblue");
    sinkText.text(d);
  });

var sinkText = svg
  .append("text")
    .attr("y", height - 25).attr("x", 24 + data.sinks.length * 24)
    .attr("class", "label sinkText")
    .text(function(d) { return data.sources[data.sinks.length - 1]; });

var rowsPerSecondOut = svg
  .append("text")
    .attr("y", height - 50).attr("x", 24 + data.sources.length * 24)
    .attr("class", "rps")
    .text("0 rps");

rowsPerSecondOut.append("title").text("rows per second");

function updateStats(data) {
  rowsPerSecondIn.text(data.rps.in + " rps");
  rowsPerSecondOut.text(data.rps.out + " rps");
}

if (typeof(Shiny) !== "undefined") {
  Shiny.addCustomMessageHandler("sparklyr_stream_view", updateStats);
}

if (data.demo) {
  setInterval(function() {
    var stats = {
      rps: {
        in: Math.floor(Math.random() * 1000),
        out: Math.floor(Math.random() * 1000)
      }
    };

    updateStats(stats);
  }, 1000);
}
