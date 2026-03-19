#!/usr/bin/env python3
"""
Script para executar DBT
"""

import os
import sys
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

try:
    from dbt.cli.main import dbtRunnerResult, dbtRunner
except ImportError:
    logger.error("dbt-core não está instalado. Instale com: pip install dbt-core dbt-duckdb")
    sys.exit(1)


class DBTRunner:
    """Wrapper para executar comandos DBT"""
    
    def __init__(self, project_dir: str = "/dbt", profiles_dir: str = "/root/.dbt"):
        self.project_dir = project_dir
        self.profiles_dir = profiles_dir
        self.runner = dbtRunner()
        logger.info(f"Inicializado DBT Runner | Projeto: {project_dir}")
    
    def run(self, 
            command: str = "run",
            select: Optional[str] = None,
            models: Optional[List[str]] = None,
            vars_dict: Optional[Dict] = None,
            threads: int = 4,
            debug: bool = False) -> bool:
        """
        Executar comando DBT
        """
        
        args = [
            command,
            "--project-dir", self.project_dir,
            "--profiles-dir", self.profiles_dir,
            "--threads", str(threads)
        ]
        
        if select:
            args.extend(["--select", select])
        
        if models:
            args.extend(["--select", " ".join(models)])
        
        if vars_dict:
            args.extend(["--vars", json.dumps(vars_dict)])
        
        if debug:
            args.append("--debug")
        
        logger.info(f"Executando: dbt {' '.join(args)}")
        
        try:
            result: dbtRunnerResult = self.runner.invoke(args)
            
            if result.returncode == 0:
                logger.info("✅ DBT executado com sucesso")
                return True
            else:
                logger.error(f"❌ DBT falhou com código: {result.returncode}")
                if result.exception:
                    logger.error(f"Exceção: {result.exception}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Erro ao executar DBT: {e}")
            return False
    
    def run_models(self, select: str = "all", vars_dict: Optional[Dict] = None) -> bool:
        """Executar modelos DBT"""
        return self.run(command="run", select=select, vars_dict=vars_dict)
    
    def test_models(self, select: str = "all") -> bool:
        """Executar testes de qualidade de dados"""
        return self.run(command="test", select=select)
    
    def generate_docs(self) -> bool:
        """Gerar documentação"""
        return self.run(command="docs generate")
    
    def clean(self) -> bool:
        """Limpar artifacts"""
        return self.run(command="clean")
    
    def debug(self) -> bool:
        """Modo debug"""
        return self.run(command="debug", debug=True)


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="DBT Runner")
    parser.add_argument("command", choices=["run", "test", "docs", "clean", "debug"],
                       help="Comando DBT a executar")
    parser.add_argument("--select", help="Seletor de modelos (ex: stg_*)")
    parser.add_argument("--vars", help="Variáveis JSON")
    parser.add_argument("--threads", type=int, default=4, help="Número de threads")
    parser.add_argument("--project-dir", default="/dbt", help="Diretório do projeto DBT")
    
    args = parser.parse_args()
    
    runner = DBTRunner(project_dir=args.project_dir)
    
    vars_dict = None
    if args.vars:
        try:
            vars_dict = json.loads(args.vars)
        except json.JSONDecodeError:
            logger.error(f"JSON inválido: {args.vars}")
            return 1
    
    if args.command == "run":
        success = runner.run_models(select=args.select or "all", vars_dict=vars_dict)
    elif args.command == "test":
        success = runner.test_models(select=args.select or "all")
    elif args.command == "docs":
        success = runner.generate_docs()
    elif args.command == "clean":
        success = runner.clean()
    elif args.command == "debug":
        success = runner.debug()
    else:
        success = False
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
