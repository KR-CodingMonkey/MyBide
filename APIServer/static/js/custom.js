$("form[name=signup_form").submit(function(e) {
    // singup form 버튼이 눌리면 발생하는 이벤트
      var $form = $(this);
      var $error = $form.find(".error");
      var data = $form.serialize();
    
      alert('click');

      $.ajax({
        // url: "/user/signup",
        url: "http://localhost:5000/user/singup",
        type: "POST",
        data: data,
        dataType: "json",
        success: function(resp) {console.log("Success"); console.log(resp);},
        error: function(resp) {console.log("Error!"); console.log(resp);}
      });
    
      e.preventDefault();
    });
