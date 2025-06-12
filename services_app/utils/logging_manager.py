import logging
import os
import socket
import json
import inspect
import datetime
import requests
import traceback
from typing import Optional, Dict, Any, List, Union

# Cấu hình SEQ
SEQ_SERVER_URL = os.getenv("SEQ_SERVER_URL", "http://localhost:5341")
SEQ_API_KEY = os.getenv("SEQ_API_KEY", "")
DEFAULT_LOG_LEVEL = logging.INFO
SERVICE_NAME = "tagging-service"
SERVICE_VERSION = "3.0.1"
DEBUG_MODE = os.getenv("SEQ_DEBUG", "false").lower() == "true"

class SeqHandler(logging.Handler):
    """
    Handler tùy chỉnh để gửi logs trực tiếp đến SEQ API
    """
    def __init__(self, url: str, api_key: str = None, batch_size: int = 10, auto_flush_timeout: int = 1, debug: bool = DEBUG_MODE):
        super().__init__()
        self.url = f"{url}/api/events/raw"
        self.api_key = api_key
        self.hostname = socket.gethostname()
        self.batch_size = batch_size
        self.auto_flush_timeout = auto_flush_timeout
        self.buffer: List[Dict[str, Any]] = []
        self.last_flush_time = datetime.datetime.now()
        self.debug = debug
        
        # Cấu hình session cho requests
        self.session = requests.Session()
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["X-Seq-ApiKey"] = api_key
        self.session.headers.update(headers)
        
        # Kiểm tra phiên bản SEQ
        self.seq_version = self._check_seq_version(url)
        
        # In thông tin khởi tạo
        if self.debug:
            print(f"\n===== SEQ HANDLER INITIALIZED =====")
            print(f"URL: {self.url}")
            print(f"API Key: {'Configured' if api_key else 'Not configured'}")
            print(f"Batch Size: {batch_size}")
            print(f"Auto Flush Timeout: {auto_flush_timeout}s")
            print(f"Debug Mode: {self.debug}")
            print(f"Host: {self.hostname}")
            print(f"SEQ Version: {self.seq_version or 'Unknown'}")
            print("==============================\n")
    
    def _check_seq_version(self, url: str) -> Optional[str]:
        """Kiểm tra phiên bản SEQ"""
        try:
            response = requests.get(f"{url}/api")
            if response.status_code == 200:
                data = response.json()
                return data.get("Version")
        except Exception:
            pass
        return None
        
    def emit(self, record: logging.LogRecord):
        """Xử lý record log và gửi đến SEQ"""
        try:
            # Tạo message từ record
            if isinstance(record.msg, str):
                message = self.format(record)
            else:
                message = str(record.msg)
            
            # Tạo message template từ message
            message_template = message
            
            # Thu thập tất cả properties
            properties = {}
            if hasattr(record, 'extra_properties') and isinstance(record.extra_properties, dict):
                for key, value in record.extra_properties.items():
                    # Đảm bảo key là chuỗi
                    prop_key = str(key)
                    
                    # Chuyển đổi các đối tượng phức tạp thành dạng có thể xử lý
                    if isinstance(value, (dict, list, tuple, set)):
                        try:
                            if isinstance(value, (list, tuple, set)):
                                properties[prop_key] = list(value)
                            else:
                                properties[prop_key] = value
                        except:
                            properties[prop_key] = str(value)
                    else:
                        properties[prop_key] = value
            
            # Thêm các thuộc tính tiêu chuẩn
            properties["SourceContext"] = record.name
            properties["service"] = SERVICE_NAME
            properties["version"] = SERVICE_VERSION
            properties["hostname"] = self.hostname
            
            # Tạo event theo định dạng SEQ 2025
            seq_event = {
                "Timestamp": datetime.datetime.now().isoformat(),
                "Level": self._get_level_name(record.levelno),
                "MessageTemplate": message_template,
                "RenderedMessage": message,
                "Properties": properties
            }
            
            # Thêm exception nếu có
            if record.exc_info:
                exception_text = self._format_exception(record.exc_info)
                seq_event["Exception"] = exception_text
            
            # Debug output
            if self.debug:
                print(f"\n----- SEQ EVENT -----")
                print(f"Message: {message}")
                print(f"Template: {message_template}")
                print(f"Level: {self._get_level_name(record.levelno)}")
                print(f"Time: {seq_event['Timestamp']}")
                print(f"Properties count: {len(properties)}")
                print("--------------------\n")
            
            # Thêm vào buffer
            self.buffer.append(seq_event)
            
            # Kiểm tra xem có nên flush không
            current_time = datetime.datetime.now()
            if (len(self.buffer) >= self.batch_size or 
                (current_time - self.last_flush_time).total_seconds() >= self.auto_flush_timeout):
                self.flush()
                
        except Exception as e:
            # Ghi log lỗi vào stderr
            import sys
            print(f"Lỗi khi xử lý log event: {e}", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
    
    def flush(self):
        """Gửi tất cả events trong buffer đến SEQ"""
        if not self.buffer:
            return
            
        try:
            # Chuẩn bị payload theo định dạng SEQ mong đợi
            payload = {
                "Events": self.buffer
            }
            
            # Debug thông tin
            if self.debug:
                print(f"\n===== FLUSHING SEQ EVENTS =====")
                print(f"URL: {self.url}")
                print(f"Events Count: {len(self.buffer)}")
                print(f"Payload Size: {len(json.dumps(payload))} bytes")
                print(f"First Event: {self.buffer[0]['RenderedMessage'] if self.buffer else 'None'}")
                print("===============================\n")
                print(f"Payload Preview: {json.dumps(payload)[:500]}...")
            
            # Gửi batch logs đến SEQ
            response = self.session.post(self.url, json=payload)
            
            if response.status_code != 201:  # SEQ trả về 201 khi thành công
                import sys
                error_msg = f"Lỗi khi gửi logs đến SEQ: HTTP {response.status_code} - {response.text}"
                print(error_msg, file=sys.stderr)
                
                if self.debug:
                    print("\n❌ SEQ API ERROR")
                    print(f"Status: {response.status_code}")
                    print(f"Response: {response.text}")
                    print(f"Request URL: {self.url}")
                    print(f"Payload Preview: {json.dumps(payload)[:200]}...")
                    print("====================\n")
            else:
                if self.debug:
                    print(f"\n✅ Gửi thành công {len(self.buffer)} events đến SEQ\n")
                
            # Xóa buffer và cập nhật thời gian flush
            self.buffer = []
            self.last_flush_time = datetime.datetime.now()
            
        except Exception as e:
            import sys
            print(f"Lỗi khi gửi logs đến SEQ: {e}", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
    
    def _get_level_name(self, level: int) -> str:
        """Chuyển đổi level number sang tên level cho SEQ"""
        if level >= logging.CRITICAL:
            return "Fatal"
        elif level >= logging.ERROR:
            return "Error"
        elif level >= logging.WARNING:
            return "Warning"
        elif level >= logging.INFO:
            return "Information"
        else:
            return "Debug"
    
    def _format_exception(self, exc_info) -> str:
        """Format exception thành chuỗi cho SEQ"""
        return "".join(traceback.format_exception(*exc_info))

class LoggingManager:
    """
    Quản lý việc cấu hình và khởi tạo logging với SEQ
    """
    def __init__(self):
        self.configured = False
        self.hostname = socket.gethostname()
        self.root_logger = logging.getLogger()
        self.seq_handler = None
    
    def configure(self, server_url: str = SEQ_SERVER_URL, api_key: str = SEQ_API_KEY):
        """
        Cấu hình logger để gửi logs đến SEQ
        """
        if self.configured:
            return
        
        # In thông tin cấu hình
        print(f"\n===== LOGGING CONFIGURATION =====")
        print(f"SEQ Server URL: {server_url}")
        print(f"API Key Configured: {'Yes' if api_key else 'No'}")
        print(f"Service Name: {SERVICE_NAME}")
        print(f"Service Version: {SERVICE_VERSION}")
        print(f"Debug Mode: {DEBUG_MODE}")
        print(f"Host: {self.hostname}")
        print("=================================\n")
        
        # Xóa tất cả handlers hiện tại
        for handler in self.root_logger.handlers[:]:
            self.root_logger.removeHandler(handler)
        
        # Cấu hình root logger
        self.root_logger.setLevel(DEFAULT_LOG_LEVEL)
        
        # Thêm console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(DEFAULT_LOG_LEVEL)
        console_formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        console_handler.setFormatter(console_formatter)
        self.root_logger.addHandler(console_handler)
        
        # Thêm file handler
        log_dir = "./logs"
        os.makedirs(log_dir, exist_ok=True)
        file_handler = logging.FileHandler(f"{log_dir}/app.log")
        file_handler.setLevel(DEFAULT_LOG_LEVEL)
        file_handler.setFormatter(console_formatter)
        self.root_logger.addHandler(file_handler)
        
        # Thêm SEQ handler
        self.seq_handler = SeqHandler(url=server_url, api_key=api_key, debug=DEBUG_MODE)
        self.seq_handler.setLevel(DEFAULT_LOG_LEVEL)
        self.root_logger.addHandler(self.seq_handler)
        
        # Ghi log khởi động
        extra_properties = {
            "server_url": server_url,
            "hostname": self.hostname,
            "service": SERVICE_NAME,
            "version": SERVICE_VERSION
        }
        
        record = logging.LogRecord(
            name="LoggingManager",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Logging configured",
            args=(),
            exc_info=None
        )
        record.extra_properties = extra_properties
        
        self.root_logger.handle(record)
        
        self.configured = True
        return True
    
    def get_logger(self, name: str) -> logging.Logger:
        """
        Lấy logger với tên chỉ định
        """
        if not self.configured:
            self.configure()
        
        return logging.getLogger(name)


class StructuredLogger:
    """
    Structured logger với metadata đi kèm
    """
    def __init__(self, name: str):
        self.name = name
        self.logger = logging.getLogger(name)
        self.hostname = socket.gethostname()
    
    def _get_caller_info(self):
        """
        Lấy thông tin về function gọi logger
        """
        frame = inspect.currentframe()
        # Bỏ qua frame hiện tại và frame của method logging
        frame = frame.f_back.f_back
        
        filename = frame.f_code.co_filename
        function = frame.f_code.co_name
        lineno = frame.f_lineno
        
        return {
            "caller_file": os.path.basename(filename),
            "caller_function": function,
            "caller_line": lineno
        }
    
    def _prepare_metadata(self, log_level: int, **kwargs) -> Dict[str, Any]:
        """
        Chuẩn bị metadata cho log message
        """
        # Tránh xung đột nếu 'level' được truyền trong kwargs
        if 'level' in kwargs:
            kwargs['user_level'] = kwargs.pop('level')
            
        metadata = {
            "hostname": self.hostname,
            "service": SERVICE_NAME,
            "version": SERVICE_VERSION,
            "timestamp": datetime.datetime.now().isoformat(),
            **self._get_caller_info(),
            **kwargs
        }
        return metadata
    
    def _log(self, level: int, message: str, **kwargs):
        """
        Phương thức cơ bản để ghi log với metadata
        """
        metadata = self._prepare_metadata(level, **kwargs)
        
        # Tạo record log
        record = logging.LogRecord(
            name=self.name,
            level=level,
            pathname="",
            lineno=0,
            msg=message,
            args=(),
            exc_info=kwargs.get('exc_info', None)
        )
        
        # Thêm metadata vào record
        record.extra_properties = metadata
        
        # Ghi log
        self.logger.handle(record)
    
    def debug(self, message: str, **kwargs):
        """Log debug message với metadata"""
        self._log(logging.DEBUG, message, **kwargs)
    
    def info(self, message: str, **kwargs):
        """Log info message với metadata"""
        self._log(logging.INFO, message, **kwargs)
    
    def warning(self, message: str, **kwargs):
        """Log warning message với metadata"""
        self._log(logging.WARNING, message, **kwargs)
    
    def error(self, message: str, **kwargs):
        """Log error message với metadata"""
        self._log(logging.ERROR, message, **kwargs)
    
    def exception(self, message: str, **kwargs):
        """Log exception message với metadata"""
        kwargs['exc_info'] = True
        self._log(logging.ERROR, message, **kwargs)
    
    def critical(self, message: str, **kwargs):
        """Log critical message với metadata"""
        self._log(logging.CRITICAL, message, **kwargs)


# Tạo singleton instance
logging_manager = LoggingManager()

def get_structured_logger(name: str) -> StructuredLogger:
    """
    Hàm tiện ích để lấy structured logger
    """
    return StructuredLogger(name)