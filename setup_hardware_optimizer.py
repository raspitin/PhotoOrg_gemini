#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PhotoOrg Hardware Setup and Optimizer
Setup una tantum per ottimizzare config.yaml basato sull'hardware disponibile
"""

import os
import sys
import time
import yaml
import tempfile
import hashlib
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any, List, Tuple, Optional
import logging
import argparse

class HardwareDetector:
    """Rileva hardware disponibile una sola volta"""
    
    def __init__(self):
        self.gpu_available = False
        self.gpu_info = {}
        self.cpu_info = {}
        self.system_info = {}
        
    def detect_all_hardware(self) -> Dict[str, Any]:
        """Rileva tutto l'hardware disponibile"""
        print("🔍 Rilevamento hardware in corso...")
        
        # CPU detection
        self.cpu_info = self._detect_cpu()
        
        # GPU detection
        self.gpu_available, self.gpu_info = self._detect_gpu()
        
        # System info
        self.system_info = self._detect_system()
        
        return {
            'gpu_available': self.gpu_available,
            'gpu_info': self.gpu_info,
            'cpu_info': self.cpu_info,
            'system_info': self.system_info
        }
    
    def _detect_cpu(self) -> Dict[str, Any]:
        """Rileva informazioni CPU"""
        cpu_count = os.cpu_count() or 4
        
        # Rileva CPU info avanzate se possibile
        try:
            import psutil
            cpu_freq = psutil.cpu_freq()
            memory = psutil.virtual_memory()
            
            cpu_info = {
                'cores': cpu_count,
                'frequency_mhz': cpu_freq.current if cpu_freq else 'Unknown',
                'memory_gb': memory.total / (1024**3),
                'memory_available_gb': memory.available / (1024**3)
            }
        except ImportError:
            cpu_info = {
                'cores': cpu_count,
                'frequency_mhz': 'Unknown',
                'memory_gb': 'Unknown',
                'memory_available_gb': 'Unknown'
            }
        
        print(f"💻 CPU rilevato: {cpu_info['cores']} cores")
        return cpu_info
    
    def _detect_gpu(self) -> Tuple[bool, Dict[str, Any]]:
        """Rileva GPU una sola volta"""
        gpu_info = {
            'available': False,
            'library': None,
            'device_count': 0,
            'memory_gb': 0,
            'name': 'No GPU',
            'compute_capability': None
        }
        
        # Test CuPy (NVIDIA CUDA)
        try:
            import cupy as cp
            
            # Test basic GPU operation
            test_array = cp.array([1, 2, 3])
            _ = cp.sum(test_array)  # Simple operation test
            
            # Get GPU info
            device = cp.cuda.Device()
            memory_info = cp.cuda.MemoryInfo()
            
            gpu_info.update({
                'available': True,
                'library': 'cupy',
                'device_count': cp.cuda.runtime.getDeviceCount(),
                'memory_gb': memory_info.total / (1024**3),
                'name': device.name.decode() if hasattr(device, 'name') else 'CUDA GPU',
                'compute_capability': device.compute_capability
            })
            
            print(f"🎮 GPU rilevata: {gpu_info['name']} ({gpu_info['memory_gb']:.1f}GB VRAM)")
            return True, gpu_info
            
        except ImportError:
            print("ℹ️ CuPy non installato - GPU NVIDIA non disponibile")
        except Exception as e:
            print(f"⚠️ GPU NVIDIA rilevata ma non funzionante: {e}")
        
        # Test altre GPU libraries se necessario (futuro: OpenCL, etc.)
        
        print("🖥️ Configurazione solo CPU")
        return False, gpu_info
    
    def _detect_system(self) -> Dict[str, Any]:
        """Rileva informazioni sistema"""
        return {
            'platform': sys.platform,
            'python_version': sys.version.split()[0],
            'architecture': os.uname().machine if hasattr(os, 'uname') else 'Unknown'
        }

class PerformanceBenchmark:
    """Benchmark performance per ottimizzazione automatica"""
    
    def __init__(self, hardware_info: Dict[str, Any]):
        self.hardware_info = hardware_info
        self.gpu_available = hardware_info['gpu_available']
        
    def run_comprehensive_benchmark(self) -> Dict[str, Any]:
        """Esegue benchmark completo e determina configurazione ottimale"""
        print("\n🧪 Avvio benchmark performance...")
        
        # Crea file di test
        test_files = self._create_test_files()
        
        results = {
            'cpu_results': {},
            'gpu_results': {},
            'optimal_config': {},
            'test_summary': {
                'test_files_count': len(test_files),
                'total_size_mb': sum(f.stat().st_size for f in test_files) / (1024*1024)
            }
        }
        
        try:
            # Test CPU con varie configurazioni worker
            results['cpu_results'] = self._benchmark_cpu(test_files)
            
            # Test GPU se disponibile
            if self.gpu_available:
                results['gpu_results'] = self._benchmark_gpu(test_files)
            
            # Determina configurazione ottimale
            results['optimal_config'] = self._determine_optimal_config(results)
            
        finally:
            # Cleanup
            self._cleanup_test_files(test_files)
        
        return results
    
    def _create_test_files(self) -> List[Path]:
        """Crea file di test realistici"""
        test_files = []
        
        print("📁 Creazione file di test...")
        
        # Mix di file come nei test reali
        file_configs = [
            (8, 500 * 1024),      # 8 file da 500KB (foto piccole)
            (5, 3 * 1024 * 1024), # 5 file da 3MB (foto medie)
            (3, 15 * 1024 * 1024) # 3 file da 15MB (file grandi/video)
        ]
        
        for count, size in file_configs:
            for i in range(count):
                temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.test')
                temp_file.write(os.urandom(size))
                temp_file.close()
                test_files.append(Path(temp_file.name))
        
        print(f"✅ Creati {len(test_files)} file di test")
        return test_files
    
    def _benchmark_cpu(self, test_files: List[Path]) -> Dict[str, Any]:
        """Benchmark CPU con varie configurazioni worker"""
        print("🖥️ Benchmark CPU...")
        
        cpu_cores = self.hardware_info['cpu_info']['cores']
        
        # Test varie configurazioni worker basate sui nostri test
        worker_configs = [4, 8, 12, 16, 20, 24, 28, 32]
        
        # Filtra configurazioni ragionevoli basate sui core
        reasonable_configs = [w for w in worker_configs if w <= cpu_cores * 2.5]
        
        best_config = {'workers': 4, 'throughput': 0, 'duration': float('inf')}
        results = {}
        
        for workers in reasonable_configs:
            print(f"   Test {workers} workers CPU...")
            
            start_time = time.time()
            processed_count = self._process_files_cpu(test_files, workers)
            duration = time.time() - start_time
            
            throughput = processed_count / duration if duration > 0 else 0
            
            results[f'{workers}_workers'] = {
                'workers': workers,
                'duration': duration,
                'throughput': throughput,
                'files_processed': processed_count
            }
            
            if throughput > best_config['throughput']:
                best_config = {
                    'workers': workers,
                    'throughput': throughput,
                    'duration': duration
                }
            
            print(f"      {throughput:.1f} file/s")
        
        results['best_cpu_config'] = best_config
        print(f"🏆 Miglior CPU: {best_config['workers']} workers ({best_config['throughput']:.1f} file/s)")
        
        return results
    
    def _benchmark_gpu(self, test_files: List[Path]) -> Dict[str, Any]:
        """Benchmark GPU con varie configurazioni"""
        print("🎮 Benchmark GPU...")
        
        try:
            import cupy as cp
            
            # Test varie configurazioni GPU
            gpu_worker_configs = [8, 12, 16, 20, 24, 28, 32]
            
            best_config = {'workers': 24, 'throughput': 0, 'duration': float('inf')}
            results = {}
            
            for workers in gpu_worker_configs:
                print(f"   Test {workers} workers GPU...")
                
                start_time = time.time()
                processed_count = self._process_files_gpu(test_files, workers)
                duration = time.time() - start_time
                
                throughput = processed_count / duration if duration > 0 else 0
                
                results[f'{workers}_workers'] = {
                    'workers': workers,
                    'duration': duration,
                    'throughput': throughput,
                    'files_processed': processed_count
                }
                
                if throughput > best_config['throughput']:
                    best_config = {
                        'workers': workers,
                        'throughput': throughput,
                        'duration': duration
                    }
                
                print(f"      {throughput:.1f} file/s")
            
            results['best_gpu_config'] = best_config
            print(f"🚀 Miglior GPU: {best_config['workers']} workers ({best_config['throughput']:.1f} file/s)")
            
            return results
            
        except Exception as e:
            print(f"❌ Errore benchmark GPU: {e}")
            return {'error': str(e)}
    
    def _process_files_cpu(self, test_files: List[Path], workers: int) -> int:
        """Processa file con CPU workers"""
        def hash_file(file_path):
            hasher = hashlib.sha256()
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hasher.update(chunk)
            return hasher.hexdigest()
        
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(hash_file, f) for f in test_files]
            results = [f.result() for f in futures]
        
        return len(results)
    
    def _process_files_gpu(self, test_files: List[Path], workers: int) -> int:
        """Processa file con GPU acceleration"""
        try:
            import cupy as cp
            
            def hash_file_gpu(file_path):
                with open(file_path, 'rb') as f:
                    data = f.read()
                
                # Simula processing GPU
                gpu_data = cp.frombuffer(data, dtype=cp.uint8)
                
                # Hash calculation (hybrid GPU/CPU)
                return hashlib.sha256(data).hexdigest()
            
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = [executor.submit(hash_file_gpu, f) for f in test_files]
                results = [f.result() for f in futures]
            
            return len(results)
            
        except Exception as e:
            print(f"⚠️ GPU processing failed: {e}")
            return 0
    
    def _determine_optimal_config(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Determina configurazione ottimale basata sui benchmark"""
        cpu_results = results.get('cpu_results', {})
        gpu_results = results.get('gpu_results', {})
        
        optimal_config = {
            'hardware_detected': {
                'gpu_available': self.gpu_available,
                'cpu_cores': self.hardware_info['cpu_info']['cores']
            }
        }
        
        if self.gpu_available and gpu_results and 'best_gpu_config' in gpu_results:
            gpu_best = gpu_results['best_gpu_config']
            cpu_best = cpu_results.get('best_cpu_config', {'throughput': 0})
            
            speedup = gpu_best['throughput'] / cpu_best['throughput'] if cpu_best['throughput'] > 0 else 1.0
            
            if speedup >= 1.5:  # GPU worth it
                optimal_config['mode'] = 'gpu'
                optimal_config['parallel_processing'] = {
                    'max_workers': gpu_best['workers'],
                    'cpu_multiplier': gpu_best['workers'] / self.hardware_info['cpu_info']['cores'],
                    'max_workers_limit': gpu_best['workers'] + 8
                }
                optimal_config['gpu_acceleration'] = {
                    'enabled': True,
                    'auto_detect': False,  # Già rilevato
                    'min_file_size_mb': 1,
                    'batch_size': 10,
                    'fallback_to_cpu': True
                }
                optimal_config['performance_metrics'] = {
                    'expected_speedup': speedup,
                    'gpu_throughput': gpu_best['throughput'],
                    'cpu_throughput': cpu_best['throughput']
                }
            else:
                # GPU non worth it, usa CPU
                optimal_config = self._cpu_only_config(cpu_results)
        else:
            # Solo CPU
            optimal_config = self._cpu_only_config(cpu_results)
        
        return optimal_config
    
    def _cpu_only_config(self, cpu_results: Dict[str, Any]) -> Dict[str, Any]:
        """Configurazione ottimale solo CPU"""
        cpu_best = cpu_results.get('best_cpu_config', {'workers': 4})
        
        return {
            'mode': 'cpu',
            'hardware_detected': {
                'gpu_available': False,
                'cpu_cores': self.hardware_info['cpu_info']['cores']
            },
            'parallel_processing': {
                'max_workers': cpu_best['workers'],
                'cpu_multiplier': cpu_best['workers'] / self.hardware_info['cpu_info']['cores'],
                'max_workers_limit': cpu_best['workers'] + 8
            },
            'gpu_acceleration': {
                'enabled': False,
                'reason': 'GPU not available or not beneficial'
            },
            'performance_metrics': {
                'cpu_throughput': cpu_best.get('throughput', 0)
            }
        }
    
    def _cleanup_test_files(self, test_files: List[Path]):
        """Pulizia file di test"""
        for file_path in test_files:
            try:
                file_path.unlink()
            except:
                pass

class ConfigWriter:
    """Scrive configurazione ottimale nel config.yaml"""
    
    def __init__(self, config_path: str = "config.yaml"):
        self.config_path = Path(config_path)
        
    def update_config_with_optimal_settings(self, optimal_config: Dict[str, Any], 
                                          hardware_info: Dict[str, Any]) -> bool:
        """Aggiorna config.yaml con impostazioni ottimali"""
        
        try:
            # Leggi config esistente
            if self.config_path.exists():
                with open(self.config_path, 'r') as f:
                    current_config = yaml.safe_load(f) or {}
            else:
                current_config = self._get_default_config()
            
            # Backup config originale
            backup_path = self.config_path.with_suffix('.yaml.backup')
            if self.config_path.exists():
                with open(backup_path, 'w') as f:
                    yaml.dump(current_config, f, default_flow_style=False)
                print(f"📁 Backup config originale: {backup_path}")
            
            # Aggiorna con configurazione ottimale
            self._merge_optimal_config(current_config, optimal_config, hardware_info)
            
            # Scrivi nuovo config
            with open(self.config_path, 'w') as f:
                yaml.dump(current_config, f, default_flow_style=False, indent=2)
            
            print(f"✅ Configurazione ottimizzata salvata: {self.config_path}")
            return True
            
        except Exception as e:
            print(f"❌ Errore scrittura config: {e}")
            return False
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Configurazione di default"""
        return {
            'source': '/path/to/source',
            'destination': '/path/to/destination',
            'database': '/path/to/database.db',  
            'log': '/path/to/log.txt',
            'supported_extensions': ['.jpg', '.jpeg', '.png', '.mp4', '.mov'],
            'image_extensions': ['.jpg', '.jpeg', '.png'],
            'video_extensions': ['.mp4', '.mov'],
            'exclude_hidden_dirs': True,
            'exclude_patterns': ['.DS_Store', 'Thumbs.db'],
            'photographic_prefixes': ['JPG', 'IMG_', 'DSC_']
        }
    
    def _merge_optimal_config(self, current_config: Dict[str, Any], 
                            optimal_config: Dict[str, Any],
                            hardware_info: Dict[str, Any]):
        """Merge configurazione ottimale in config esistente"""
        
        # Aggiungi commento con info hardware
        current_config['# Hardware Auto-Detection Results'] = f"Generated on {time.strftime('%Y-%m-%d %H:%M:%S')}"
        current_config['# System Info'] = {
            'cpu_cores': hardware_info['cpu_info']['cores'],
            'gpu_available': hardware_info['gpu_available'],
            'gpu_name': hardware_info['gpu_info'].get('name', 'No GPU'),
            'configuration_mode': optimal_config['mode']
        }
        
        # Merge parallel processing settings
        current_config['parallel_processing'] = optimal_config.get('parallel_processing', {})
        
        # Merge GPU settings se applicabili
        if 'gpu_acceleration' in optimal_config:
            current_config['gpu_acceleration'] = optimal_config['gpu_acceleration']
        
        # Add performance metrics per reference
        if 'performance_metrics' in optimal_config:
            current_config['# Performance Expectations'] = optimal_config['performance_metrics']

def main():
    """Funzione principale per --setupGpu"""
    parser = argparse.ArgumentParser(
        description="PhotoOrg Hardware Setup and Optimizer",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--setupGpu', 
        action='store_true',
        help='Rileva hardware e ottimizza automaticamente config.yaml'
    )
    
    parser.add_argument(
        '--config',
        default='config.yaml',
        help='Path del file config.yaml da ottimizzare (default: config.yaml)'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true', 
        help='Output dettagliato durante il setup'
    )
    
    args = parser.parse_args()
    
    if not args.setupGpu:
        parser.print_help()
        return
    
    # Setup logging
    if args.verbose:
        logging.basicConfig(level=logging.INFO)
    
    print("🚀 PhotoOrg Hardware Setup e Ottimizzazione")
    print("=" * 50)
    
    # Step 1: Hardware Detection
    detector = HardwareDetector()
    hardware_info = detector.detect_all_hardware()
    
    # Step 2: Performance Benchmark  
    benchmark = PerformanceBenchmark(hardware_info)
    benchmark_results = benchmark.run_comprehensive_benchmark()
    
    # Step 3: Config Writing
    config_writer = ConfigWriter(args.config)
    success = config_writer.update_config_with_optimal_settings(
        benchmark_results['optimal_config'], 
        hardware_info
    )
    
    # Step 4: Report Results
    print("\n" + "=" * 50)
    print("📊 RISULTATI OTTIMIZZAZIONE")
    print("=" * 50)
    
    optimal = benchmark_results['optimal_config']
    
    if optimal['mode'] == 'gpu':
        print("🎮 CONFIGURAZIONE GPU OTTIMALE")
        metrics = optimal.get('performance_metrics', {})
        print(f"   🚀 Speedup atteso: {metrics.get('expected_speedup', 0):.1f}x")
        print(f"   ⚡ Throughput GPU: {metrics.get('gpu_throughput', 0):.1f} file/s")
        print(f"   🖥️ Throughput CPU: {metrics.get('cpu_throughput', 0):.1f} file/s")
        print(f"   🔧 Workers ottimali: {optimal['parallel_processing']['max_workers']}")
    else:
        print("🖥️ CONFIGURAZIONE CPU OTTIMALE")
        metrics = optimal.get('performance_metrics', {})
        print(f"   ⚡ Throughput: {metrics.get('cpu_throughput', 0):.1f} file/s")
        print(f"   🔧 Workers ottimali: {optimal['parallel_processing']['max_workers']}")
        print(f"   📝 Motivo: {optimal['gpu_acceleration'].get('reason', 'CPU only')}")
    
    if success:
        print(f"\n✅ Configurazione salvata in: {args.config}")
        print("🎯 PhotoOrg è ora ottimizzato per il tuo hardware!")
        print("\nPer usare la configurazione ottimizzata:")
        print("python3 PhotoOrg.py")
    else:
        print(f"\n❌ Errore nel salvare la configurazione")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
