// The only JavaScript on the site. Converts UTC <time> elements to the
// visitor's local timezone. With JS disabled the UTC fallback text stays.
document.querySelectorAll("time[datetime]").forEach(function (el) {
    var d = new Date(el.getAttribute("datetime"));
    if (isNaN(d.getTime())) {
        return;
    }
    var parts = new Intl.DateTimeFormat(undefined, {
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        hourCycle: "h23",
        timeZoneName: "short"
    }).formatToParts(d);
    var p = {};
    parts.forEach(function (part) {
        p[part.type] = part.value;
    });
    var label = p.year + "-" + p.month + "-" + p.day + " " + p.hour + ":" + p.minute;
    if (p.timeZoneName) {
        label += " " + p.timeZoneName;
    }
    el.textContent = label;
});
