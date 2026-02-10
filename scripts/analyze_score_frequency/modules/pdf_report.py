"""
Modulo per la generazione automatica di report PDF 
contenenti tutti i grafici matplotlib prodotti durante l'analisi.
"""

import os
from datetime import datetime
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt


class PdfReport:
    """
    Classe per raccogliere automaticamente le figure matplotlib
    e salvarle in un unico file PDF.
    
    Uso tipico:
        report = PdfReport("output_report.pdf")
        
        # ... genera i tuoi grafici con plt.figure() / plt.plot() ecc ...
        fig1, ax1 = plt.subplots()
        ax1.plot(...)
        report.add_figure(fig1)
        
        # Oppure cattura automaticamente la figura corrente:
        plt.figure()
        plt.plot(...)
        report.add_current_figure()
        
        # Alla fine, salva tutto
        report.save()
    """
    
    def __init__(self, output_path=None, output_dir=None):
        """
        Inizializza il report PDF.
        
        Args:
            output_path: Percorso completo del file PDF di output.
                         Se None, verrà generato automaticamente con timestamp.
            output_dir: Directory in cui salvare il PDF (usata solo se output_path è None).
                        Default: directory 'graphics' accanto al notebook.
        """
        self._figures = []
        
        if output_path is None:
            if output_dir is None:
                # Default: cartella 'graphics' nella stessa directory del modulo parent
                output_dir = os.path.join(
                    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                    'graphics'
                )
            
            os.makedirs(output_dir, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = os.path.join(output_dir, f"report_{timestamp}.pdf")
        
        self._output_path = output_path
    
    def add_figure(self, fig=None, title=None):
        """
        Aggiunge una figura matplotlib al report.
        
        Args:
            fig: Oggetto matplotlib Figure. Se None, usa la figura corrente (plt.gcf()).
            title: Titolo opzionale da aggiungere come suptitle alla figura.
        """
        if fig is None:
            fig = plt.gcf()
        
        if title is not None:
            fig.suptitle(title, fontsize=14, fontweight='bold')
        
        self._figures.append(fig)
    
    def add_current_figure(self, title=None):
        """
        Aggiunge la figura matplotlib corrente al report.
        Alias per add_figure() senza argomenti.
        
        Args:
            title: Titolo opzionale da aggiungere come suptitle alla figura.
        """
        self.add_figure(fig=None, title=title)
    
    def save(self, close_figures=True):
        """
        Salva tutte le figure raccolte in un unico file PDF.
        
        Args:
            close_figures: Se True (default), chiude le figure dopo il salvataggio 
                           per liberare memoria.
        
        Returns:
            str: Percorso del file PDF salvato, o None se non ci sono figure.
        """
        if not self._figures:
            print("⚠️  Nessuna figura da salvare nel report PDF.")
            return None
        
        with PdfPages(self._output_path) as pdf:
            for fig in self._figures:
                pdf.savefig(fig, bbox_inches='tight')
                if close_figures:
                    plt.close(fig)
        
        n_figures = len(self._figures)
        self._figures = []  # Reset della lista
        
        print(f"✅ Report PDF salvato con successo!")
        print(f"   📄 File: {self._output_path}")
        print(f"   📊 Grafici inclusi: {n_figures}")
        
        return self._output_path
    
    @property
    def figure_count(self):
        """Ritorna il numero di figure attualmente raccolte."""
        return len(self._figures)
    
    @property
    def output_path(self):
        """Ritorna il percorso del file PDF di output."""
        return self._output_path
