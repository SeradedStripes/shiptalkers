// Admin config only. Show/hide buttons reveal secret values in place.
document.querySelectorAll(".reveal-btn").forEach(function (btn) {
    btn.addEventListener("click", function () {
        var input = document.getElementById(btn.getAttribute("data-target"));
        if (!input) {
            return;
        }
        var show = input.type === "password";
        input.type = show ? "text" : "password";
        btn.textContent = show ? "Hide" : "Show";
    });
});
