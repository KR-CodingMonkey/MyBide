
// document.getElementById('submit').onclick = function() {

function Login(){
    var ID = document.getElementById('user_email').value;
    var pass = document.getElementById('user_pw').value;

    if (ID === "admin" && pass === "1234") {
        alert("로그인 성공!");
        window.location.href = "./main.html";
    }
    else {
        alert("로그인 실패!");
    }
}
