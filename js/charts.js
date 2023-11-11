google.charts.load('current', { packages: ['corechart', 'bar', 'table'] });

function drawChart(inData, inTitle, location) {

    var data = google.visualization.arrayToDataTable(inData);

    var options = {
        title: inTitle,
        pieHole: 0.4,
    };

    var chart = new google.visualization.PieChart(document.getElementById(location));
    chart.draw(data, options);
}

function drawMultSeries(inData, inTitle, location, xLabel, yLabel) {
    var data = google.visualization.arrayToDataTable(inData);

    var options = {
        title: inTitle,
        chartArea: { width: '50%' },
        hAxis: {
            title: xLabel,
            minValue: 0
        },
        vAxis: {
            title: yLabel
        }
    };

    var chart = new google.visualization.BarChart(document.getElementById(location));
    chart.draw(data, options);
}

function drawBarChart(inData, inTitle) {
    var data = google.visualization.arrayToDataTable(inData);

    var view = new google.visualization.DataView(data);
    view.setColumns([0, 1,
        {
            calc: "stringify",
            sourceColumn: 1,
            type: "string",
            role: "annotation"
        },
        2]);

    var options = {
        title: inTitle,
        width: 600,
        height: 400,
        bar: { groupWidth: "95%" },
        legend: { position: "none" },
    };
    var chart = new google.visualization.BarChart(document.getElementById("barchart_values"));
    chart.draw(view, options);
}

function drawColumnChart1(inData) {
    var data = google.visualization.arrayToDataTable(inData);

    var options = {
        chart: {
            title: 'Company Performance',
            subtitle: 'Sales, Expenses, and Profit: 2014-2017',
        }
    };

    var chart = new google.charts.Bar(document.getElementById('columnchart_material'));

    chart.draw(data, google.charts.Bar.convertOptions(options));
}

function drawColumnChart2() {
    var data = google.visualization.arrayToDataTable([
        ["Element", "Density", { role: "style" }],
        ["Copper", 8.94, "#b87333"],
        ["Silver", 10.49, "silver"],
        ["Gold", 19.30, "gold"],
        ["Platinum", 21.45, "color: #e5e4e2"]
    ]);

    var view = new google.visualization.DataView(data);
    view.setColumns([0, 1,
        {
            calc: "stringify",
            sourceColumn: 1,
            type: "string",
            role: "annotation"
        },
        2]);

    var options = {
        title: "Density of Precious Metals, in g/cm^3",
        width: 600,
        height: 400,
        bar: { groupWidth: "95%" },
        legend: { position: "none" },
    };
    var chart = new google.visualization.ColumnChart(document.getElementById("columnchart_values"));
    chart.draw(view, options);
}

function drawTableChart() {
    var data = new google.visualization.DataTable();
    data.addColumn('string', 'Name');
    data.addColumn('number', 'Salary');
    data.addColumn('boolean', 'Full Time Employee');
    data.addRows([
        ['Mike', { v: 10000, f: '$10,000' }, true],
        ['Jim', { v: 8000, f: '$8,000' }, false],
        ['Alice', { v: 12500, f: '$12,500' }, true],
        ['Bob', { v: 7000, f: '$7,000' }, true]
    ]);

    var table = new google.visualization.Table(document.getElementById('table_div'));

    // table.draw(data, { showRowNumber: true, width: '100%', height: '100%' });
    table.draw(data, { showRowNumber: true });

}