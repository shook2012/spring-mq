package com.sk.mq.support;

import com.sk.mq.annotation.AsyncMQ;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.AnnotatedBeanDefinition;
import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.context.annotation.ClassPathBeanDefinitionScanner;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.core.type.classreading.MetadataReader;
import org.springframework.core.type.classreading.MetadataReaderFactory;
import org.springframework.core.type.filter.TypeFilter;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

/**
 * Created by samfan on 2017-04-22.
 */
@Slf4j
public class RocketMQInterfaceScanner extends ClassPathBeanDefinitionScanner {

    private String producerBeanName;

    private ProducerRegistry producerRegistry = new ProducerRegistry();

    public void setProducerBeanName(String producerBeanName) {
        this.producerBeanName = producerBeanName;
    }

    public RocketMQInterfaceScanner(BeanDefinitionRegistry registry) {
        super(registry);
        if(registry instanceof ConfigurableBeanFactory){
            producerRegistry.setBeanFactory((ConfigurableBeanFactory)registry);
        }
    }

    @Override
    protected void registerDefaultFilters() {

        this.addIncludeFilter(new TypeFilter() {
            @Override
            public boolean match(MetadataReader metadataReader, MetadataReaderFactory metadataReaderFactory) throws IOException {
                return true;
            }
        });

        this.addExcludeFilter(new TypeFilter() {
            @Override
            public boolean match(MetadataReader metadataReader, MetadataReaderFactory metadataReaderFactory) throws IOException {
                String className = metadataReader.getClassMetadata().getClassName();
                return className.endsWith("package-info");
            }
        });
    }

    @Override
    protected void postProcessBeanDefinition(AbstractBeanDefinition beanDefinition, String beanName) {
        if(beanDefinition instanceof AnnotatedBeanDefinition){
            AnnotatedBeanDefinition definition = (AnnotatedBeanDefinition) beanDefinition;
            String beanClassName = definition.getMetadata().getClassName();
            Class<?> cls = ClassUtils.resolveClassName(beanClassName,Thread.currentThread().getContextClassLoader());
            producerRegistry.addMapper(cls);
        }
        super.postProcessBeanDefinition(beanDefinition, beanName);
    }


    @Override
    protected Set<BeanDefinitionHolder> doScan(String... basePackages) {
        Set<BeanDefinitionHolder> beanDefinitions = super.doScan(basePackages);
        if(beanDefinitions.isEmpty()){
            log.warn("No interface was found in '" + Arrays.toString(basePackages) + "' package. Please check your configuration.");
        }else{
            for (BeanDefinitionHolder holder : beanDefinitions) {
                GenericBeanDefinition definition = (GenericBeanDefinition) holder.getBeanDefinition();
                log.debug("Creating ProducerFactoryBean with name '" + holder.getBeanName() + "' and '" + definition.getBeanClassName() + "' mqInterface");

                // the mapper interface is the original class of the bean
                // but, the actual class of the bean is ProducerFactoryBean
                //definition.getPropertyValues().add("mqInterface", definition.getBeanClassName());
                definition.getConstructorArgumentValues().addGenericArgumentValue(definition.getBeanClassName());

                definition.setBeanClass(ProducerFactoryBean.class);

                definition.getPropertyValues().add("producerRegistry",producerRegistry);

                boolean explicitFactoryUsed = false;
                if (StringUtils.hasText(this.producerBeanName)) {
                    definition.getPropertyValues().add("producer", new RuntimeBeanReference(this.producerBeanName));
                    explicitFactoryUsed = true;
                }

                if (!explicitFactoryUsed) {
                    log.debug("Enabling autowire by type for MapperFactoryBean with name '" + holder.getBeanName() + "'.");
                    definition.setAutowireMode(AbstractBeanDefinition.AUTOWIRE_BY_TYPE);
                }
            }
        }
        return beanDefinitions;
    }

    @Override
    public void setResourceLoader(ResourceLoader resourceLoader) {
        super.setResourceLoader(resourceLoader);
    }

    @Override
    protected void registerBeanDefinition(BeanDefinitionHolder definitionHolder, BeanDefinitionRegistry registry) {
        super.registerBeanDefinition(definitionHolder, registry);
    }

    @Override
    protected boolean isCandidateComponent(AnnotatedBeanDefinition beanDefinition) {
        AnnotationMetadata metaData = beanDefinition.getMetadata();
        return (metaData.isInterface() && metaData.isIndependent() && metaData.hasAnnotatedMethods(AsyncMQ.class.getName()));
    }

}
