// !preview r2d3 data=list(sources = list("FileStreamSource[file]"), sinks = list("FileSink[file]"))

var barHeight = Math.ceil(height / data.length);

var sources = svg.selectAll(".source")
  .data(data.sources)
  .enter()
  .append("g")
    .attr("class", "source")
    .attr("transform", function(d, i) {
      return "translate(" + (30 + i * 24) + ", 30)";
    })
    .append("circle")
    .attr("cx", "0").attr("cy", "0").attr("r", "10")
    .attr("stroke", "white")
    .attr("stroke-width", "2")
    .attr("fill", "orange");

var sourceText = svg
  .append("text")
    .attr("y", 35).attr("x", 24 + data.sources.length * 24)
    .attr("class", "label")
    .text(function(d) { return data.sources[data.sources.length - 1]; });

var sinks = svg.selectAll(".sink")
  .data(data.sinks)
  .enter()
  .append("g")
    .attr("class", "sink")
    .attr("transform", function(d, i) {
      return "translate(" + (30 + i * 24) + ","  + (height - 30) + ")";
    })
    .append("circle")
    .attr("cx", "0").attr("cy", "0").attr("r", "10")
    .attr("stroke", "white")
    .attr("stroke-width", "2")
    .attr("fill", "steelblue");

var sinkText = svg
  .append("text")
    .attr("y", height - 25).attr("x", 24 + data.sources.length * 24)
    .attr("class", "label")
    .text(function(d) { return data.sources[data.sinks.length - 1]; });
