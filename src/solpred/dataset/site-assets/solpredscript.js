class Solpred {
  static change_elem(id, contents) {
    document.getElementById(id).innerHTML = contents;
  }

  static change_title(title) {
    document.title = title;
  }

  static add_stylesheet(href) {
    let link = document.createElement('link');
    link.href = href;
    link.rel = 'stylesheet';
    link.type = 'text/css';
    document.head.appendChild(link);
  }
}