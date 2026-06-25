import { HashRouter, Routes, Route } from "react-router-dom";
import Navbar from "./components/Navbar";
import Footer from "./components/Footer";
import HomePage from "./pages/HomePage";
import PerformancePage from "./pages/PerformancePage";

// HashRouter keeps deep links (#/performance) working on GitHub Pages with zero server config.
export default function App() {
  return (
    <HashRouter>
      <div className="flex min-h-full flex-col bg-bg">
        <Navbar />
        <main className="flex-1">
          <Routes>
            <Route path="/" element={<HomePage />} />
            <Route path="/performance" element={<PerformancePage />} />
            <Route path="*" element={<HomePage />} />
          </Routes>
        </main>
        <Footer />
      </div>
    </HashRouter>
  );
}
