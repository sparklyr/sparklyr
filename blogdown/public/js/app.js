
$(document).ready(function() {
  
  initializeSideMenu();

  initializeAnchorLinks();

  initializeMobileMenu();
  
  initializeSearch();

  initializeScrollSpy();
  
});

function initializeSideMenu() {
  
  var firstPartUrl = window.location.pathname.split('/')[1];
  $('.side-menu-list').each(function () {
    var parentUrl = $(this).data('parenturl');
    var firstPartUrlMenu = parentUrl.split('/')[1];
    if (firstPartUrl === firstPartUrlMenu) {
      $(this).css('display', 'block');
    }
  });

  $('.top-menu-item').each(function () {
    var parentUrl = $(this).data('parenturl');
    var firstPartUrlMenu = parentUrl.split('/')[1];
    $(this).removeClass('active');
    if (firstPartUrl === firstPartUrlMenu) {
      $(this).addClass('active');
    }
    if (firstPartUrlMenu === "blog.html") {
      if (firstPartUrl === "blog" || firstPartUrl === "categories")
        $(this).addClass('active');
    }
  });

  $('.side-menu__sub__item__text').each(function () {
    $(this).css('display', 'block');
  });

}

function initializeAnchorLinks() {
  // Hoverable anchors on all h2 elements
  $('.content').find('h2, h3, h4').each(function() {
    var id = $(this).attr('id');
    if (!id)
      id = $(this).parent().attr('id');
    $(this).append('<a class="anchor-hover" href="'+ window.location.pathname +'#'+id + 
                   '"><i class="fa fa-link" aria-hidden="true"></i></a>');
  });
}

function initializeMobileMenu() {
  
  $('#menu-toggle').on('click', function (e) {
    $('.top-menu-items').toggleClass('open');
  });

  $('top-menu-item').on('click', function(e) {
    $('.top-menu-items').removeClass('open');
  });

  $('#mobile-menu li').click(function() {
    $(this).toggleClass('active');
    $(this).next('ul').toggleClass('show');
  });

  $('#mobile-header .hamburger').click(function() {
    $('#mobile-menu').toggleClass('show');
    $('#mobile-menu-container').toggleClass('show');
    if ($('#mobile-menu').hasClass('show')) {
      $(document.body).addClass('noscroll');
      $(document.documentElement).addClass('noscroll');
    } else {
      $(document.body).removeClass('noscroll');
      $(document.documentElement).removeClass('noscroll');
    }
  });
}


function initializeSearch() {
  
  $('.search-button').click(function(event) {
    $('.search-bar').toggleClass('active');
    $('.search-bar__input input').focus();
  });

  $('.search-bar__exit').click(function(event) {
    exitSearch(event);
  });

  $('.search-bar__input input').keyup(function(e) {
      //escape key
      if (e.keyCode == 27)
        exitSearch(e);
  });

  function exitSearch(event) {
    event && event.preventDefault();
    event && event.stopPropagation();
    $('.search-bar__input input').val('');
    $('.search-bar__input input').focusout();
    $('.search-bar').toggleClass('active');
  }
}


function initializeScrollSpy() {
  
  // if we have a gumshoe container then populate it
  var gumshoeContainer = $('#gumshoe-container');
  $('.content').find('h2').each(function() {
    var id = $(this).attr('id');
    if (!id)
      id = $(this).parent().attr('id');
    if (id) {
      var li = $('<li></li>');
      var a = $('<a></a>');
      a.attr('href', "#" + id);
      a.text($(this).text());
      li.append(a);
      gumshoeContainer.append(li);
    }
  });
  
  
  // initialize gumshoe
  // https://github.com/cferdinandi/gumshoe
  gumshoe.init({
    offset: 50
  });
  
  // check for empty toc 
  var ul = $('ul[data-gumshoe]');
  if (ul.children().length === 0)
    $(".markdowned .doc-page-body").css('padding-right', 0);
    
  // check for no-toc 
  var no_toc = $(".markdowned .doc-page-body .no-toc");
  if (no_toc.length > 0) {
    no_toc.parent().css('padding-right', 0);
    no_toc.parent().parent().children('.doc-page-index').css('display', 'none');
  }
}




