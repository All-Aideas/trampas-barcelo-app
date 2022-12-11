import { initializeApp } from "https://www.gstatic.com/firebasejs/9.10.0/firebase-app.js";
import { getAuth } from "https://www.gstatic.com/firebasejs/9.10.0/firebase-auth.js"
import { signInWithEmailAndPassword } from "https://www.gstatic.com/firebasejs/9.10.0/firebase-auth.js"
import { signOut } from "https://www.gstatic.com/firebasejs/9.10.0/firebase-auth.js"
import { onAuthStateChanged } from "https://www.gstatic.com/firebasejs/9.10.0/firebase-auth.js"
import { FIREBASE_API_KEY, FIREBASE_AUTHDOMAIN, FIREBASE_PROJECTID, FIREBASE_STORAGEBUCKET, FIREBASE_MESSAGINGSENDERID, FIREBASE_API_ID, FIREBASE_MEASUREMENTID } from './config.js';


const firebaseConfig = {
    apiKey: FIREBASE_API_KEY,
    authDomain: FIREBASE_AUTHDOMAIN,
    projectId: FIREBASE_PROJECTID,
    storageBucket: FIREBASE_STORAGEBUCKET,
    messagingSenderId: FIREBASE_MESSAGINGSENDERID,
    appId: FIREBASE_API_ID,
    measurementId: FIREBASE_MEASUREMENTID
};

const loggedOutLinks = document.querySelectorAll(".logged-out");
const loggedInLinks = document.querySelectorAll(".logged-in");
const imagenPortada = document.querySelectorAll(".imagen-portada");
const mapaDashboard = document.querySelectorAll(".mapa-dashboard");
const detalleCasos = document.querySelectorAll(".detalle-casos");

loggedInLinks.forEach((link) => (link.style.display = "none"));
loggedOutLinks.forEach((link) => (link.style.display = "block"));
imagenPortada.forEach((link) => (link.style.display = "block"));
mapaDashboard.forEach((link) => (link.style.display = "none"));
detalleCasos.forEach((link) => (link.style.display = "none"));

export const loginCheck = (user) => {
  loggedInLinks.forEach((link) => (link.style.display = "none"));
  loggedOutLinks.forEach((link) => (link.style.display = "block"));
  imagenPortada.forEach((link) => (link.style.display = "block"));
  mapaDashboard.forEach((link) => (link.style.display = "none"));
  detalleCasos.forEach((link) => (link.style.display = "none"));
  if (user) {
    loggedInLinks.forEach((link) => (link.style.display = "block"));
    loggedOutLinks.forEach((link) => (link.style.display = "none"));
    imagenPortada.forEach((link) => (link.style.display = "none"));
    mapaDashboard.forEach((link) => (link.style.display = "block"));
    detalleCasos.forEach((link) => (link.style.display = "block"));
  }
};

// Initialize Firebase
export const app = initializeApp(firebaseConfig);
export const auth = getAuth(app)

const signInForm = document.querySelector("#login-form");

function showMessage(message, type = "success"){
  Toastify({
    text: message,
    duration: 3000,
    destination: "https://github.com/apvarun/toastify-js",
    newWindow: true,
    close: true,
    gravity: "bottom", // `top` or `bottom`
    position: "right", // `left`, `center` or `right`
    stopOnFocus: true, // Prevents dismissing of toast on hover
    style: {
      background: type === "success" ? "green" : "red",
    },
    // onClick: function () { } // Callback after click
  }).showToast();
}

signInForm.addEventListener("submit", async (e) => {
  e.preventDefault();
  const email = signInForm["login-email"].value;
  const password = signInForm["login-password"].value;

  try {
	const userCredentials = await signInWithEmailAndPassword(auth, email, password)
    
    // Close the login modal
    const modal = bootstrap.Modal.getInstance(signInForm.closest('.modal'));
    modal.hide();

    // reset the form
    signInForm.reset();

    // show welcome message
    showMessage("Bienvenido " + userCredentials.user.email);
  } catch (error) {
	console.log(error);
    if (error.code === 'auth/wrong-password') {
      showMessage("Wrong password", "error")
    } else if (error.code === 'auth/user-not-found') {
      showMessage("Usuario no encontrado.", "error")
    } else {
      showMessage("Ha ocurrido un error.", "error")
    }
  }
});

// list for auth state changes
onAuthStateChanged(auth, async (user) => {
  loginCheck(user);
  if (user) {
    loginCheck(user);
    try {
      console.log("Login");
    } catch (error) {
      console.log(error)
    }
  }
});

const logout = document.querySelector("#logout");

logout.addEventListener("click", async (e) => {
  e.preventDefault();
  try {
    await signOut(auth)
    console.log("sign out");
  } catch (error) {
    console.log(error)
  }
});
