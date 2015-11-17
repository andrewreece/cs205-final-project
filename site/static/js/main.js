


function getData(table_name) {
	console.log('in getData');
	// make now playing carousel
	d3.json('/pull/'+table_name, function(error,data) {

		display(data);
	});
	
}

function display(d) {
	console.log(d);
	console.log(Object.keys(d));
	console.log(d[1].text);
	d3.selectAll(".tweet")
		.data(d3.entries(d))
		.enter()
		.append("div")
			.attr("class","tweet")
			.style("width","500px")
			.style("height","300px")
			.style("display","block")
			.text( function(d,i) { console.log(d3.entries(d.value)); return JSON.stringify(d.value); });
}

$('#pull').click( function() { getData('test'); } );

